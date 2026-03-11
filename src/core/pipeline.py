# -*- coding: utf-8 -*-
"""
===================================
全球宏观智能分析系统 - 核心分析流水线
===================================
"""

import logging
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

from src.config import get_config, Config
from src.storage import get_db
from data_provider import DataFetcherManager
from data_provider.realtime_types import ChipDistribution
from src.analyzer import GeminiAnalyzer, AnalysisResult
from src.notification import NotificationService, NotificationChannel
from src.search_service import SearchService
from src.enums import ReportType
from src.stock_analyzer import StockTrendAnalyzer, TrendAnalysisResult
from src.core.trading_calendar import get_market_for_stock, is_market_open
from bot.models import BotMessage

logger = logging.getLogger(__name__)

class StockAnalysisPipeline:
    def __init__(
        self, config: Optional[Config] = None, max_workers: Optional[int] = None,
        source_message: Optional[BotMessage] = None, query_id: Optional[str] = None,
        query_source: Optional[str] = None, save_context_snapshot: Optional[bool] = None
    ):
        self.config = config or get_config()
        self.max_workers = max_workers or self.config.max_workers
        self.source_message = source_message
        self.query_id = query_id
        self.query_source = query_source or "system"
        self.save_context_snapshot = self.config.save_context_snapshot if save_context_snapshot is None else save_context_snapshot
        
        self.db = get_db()
        self.fetcher_manager = DataFetcherManager()
        self.trend_analyzer = StockTrendAnalyzer()
        self.analyzer = GeminiAnalyzer()
        self.notifier = NotificationService(source_message=source_message)
        
        self.search_service = SearchService(
            bocha_keys=self.config.bocha_api_keys, tavily_keys=self.config.tavily_api_keys,
            brave_keys=self.config.brave_api_keys, serpapi_keys=self.config.serpapi_keys,
            news_max_age_days=self.config.news_max_age_days,
        )

    def _get_fear_and_greed_index(self):
        """华尔街宏观插件：获取加密市场恐慌与贪婪指数"""
        import requests
        try:
            resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
            data = resp.json()
            val = data['data'][0]['value']
            cls = data['data'][0]['value_classification']
            return f"当前指数: {val} ({cls}) - [0极端恐慌, 100极端贪婪]"
        except Exception:
            return "宏观情绪指数获取失败"

    def _get_crypto_data(self, code: str, days: int = 150):
        """Kraken 历史K线引擎 (无视封锁，生成150天数据以算均线)"""
        import requests
        from datetime import datetime
        symbol = code.upper().replace("-USD", "USD").replace("-", "") 
        url = f"https://api.kraken.com/0/public/OHLC?pair={symbol}&interval=1440"
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            res = requests.get(url, headers=headers, timeout=10)
            data = res.json()
            if data.get('error'): return None
                
            result = data.get('result', {})
            pair_key =[k for k in result.keys() if k != 'last'][0]
            klines = result[pair_key]
            
            records = []
            for row in klines[-days:]:
                dt_obj = datetime.fromtimestamp(row[0]).date()
                records.append({
                    "date": dt_obj,
                    "trade_date": dt_obj, # 💡 趋势引擎必须的列
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[6]),
                    "amount": float(row[4]) * float(row[6]),
                    "turnover": 0.0,
                    "code": code
                })
            df = pd.DataFrame(records)
            df['pct_chg'] = df['close'].pct_change() * 100
            df['pct_chg'] = df['pct_chg'].fillna(0)
            return df
        except Exception as e:
            logger.error(f"Kraken 历史获取异常: {e}")
            return None

    def _get_crypto_realtime(self, code: str):
        """Kraken 实时现价引擎"""
        import requests
        symbol = code.upper().replace("-USD", "USD").replace("-", "")
        url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers, timeout=10)
            data = resp.json()
            if data.get('error'): return None
            
            result = data.get('result', {})
            pair_key = list(result.keys())[0]
            ticker = result[pair_key]
            
            class CryptoQuote: pass
            quote = CryptoQuote()
            quote.name = code.replace("-USD", "")
            quote.price = float(ticker['c'][0])
            quote.open_price = float(ticker['o'])
            quote.high = float(ticker['h'][0])
            quote.low = float(ticker['l'][0])
            quote.volume = float(ticker['v'][0])
            quote.amount = quote.volume * quote.price
            quote.pre_close = float(ticker['o'])
            quote.change_pct = (quote.price - quote.pre_close) / quote.pre_close * 100
            quote.turnover_rate = None
            quote.volume_ratio = 1.0
            return quote
        except Exception as e:
            return None

    def fetch_and_save_stock_data(self, code: str, force_refresh: bool = False) -> Tuple[bool, Optional[str]]:
        try:
            stock_name = self.fetcher_manager.get_stock_name(code)
            
            # 🚀 加密货币跳过本地数据库，直连内存引擎
            if "-USD" in code.upper():
                logger.info(f"{stock_name}({code}) 🚀 加密货币跳过数据库，启用内存计算")
                return True, None

            today = date.today()
            if not force_refresh and self.db.has_today_data(code, today):
                return True, None

            df, source_name = self.fetcher_manager.get_daily_data(code, days=60)
            if df is None or df.empty: return False, "获取数据为空"

            self.db.save_daily_data(df, code, source_name)
            return True, None
        except Exception as e:
            return False, str(e)
    
    def analyze_stock(self, code: str, report_type: ReportType, query_id: str) -> Optional[AnalysisResult]:
        try:
            stock_name = self.fetcher_manager.get_stock_name(code) or f'股票{code}'

            # Step 1: 获取实时行情
            realtime_quote = None
            try:
                if "-USD" in code.upper():
                    realtime_quote = self._get_crypto_realtime(code)
                else:
                    realtime_quote = self.fetcher_manager.get_realtime_quote(code)
                if realtime_quote and hasattr(realtime_quote, 'name') and realtime_quote.name:
                    stock_name = realtime_quote.name
            except Exception as e:
                logger.warning(f"获取实时行情失败: {e}")

            # Step 2: 获取筹码分布
            chip_data = None
            if "-USD" not in code.upper():
                try: chip_data = self.fetcher_manager.get_chip_distribution(code)
                except Exception: pass

            use_agent = getattr(self.config, 'agent_mode', False)
            if use_agent:
                return self._analyze_with_agent(code, report_type, query_id, stock_name, realtime_quote, chip_data)
            
            # Step 3: 趋势分析
            trend_result = None
            try:
                if "-USD" in code.upper():
                    df_crypto = self._get_crypto_data(code, days=150)
                    if df_crypto is not None and not df_crypto.empty:
                        # 注入实时最新价
                        if realtime_quote and hasattr(realtime_quote, 'price') and realtime_quote.price > 0:
                            last_date = df_crypto['date'].iloc[-1]
                            if last_date < date.today():
                                new_row = {
                                    'date': date.today(), 'trade_date': date.today(),
                                    'open': realtime_quote.open_price, 'high': realtime_quote.high,
                                    'low': realtime_quote.low, 'close': realtime_quote.price,
                                    'volume': getattr(realtime_quote, 'volume', 0),
                                    'amount': getattr(realtime_quote, 'amount', 0),
                                    'pct_chg': getattr(realtime_quote, 'change_pct', 0),
                                    'turnover': 0.0, 'code': code
                                }
                                df_crypto = pd.concat([df_crypto, pd.DataFrame([new_row])], ignore_index=True)
                            else:
                                df_crypto.loc[df_crypto.index[-1], 'close'] = realtime_quote.price

                        # 💡 核心修复：纯手工算好 MA5, 10, 20, 60，喂饱分析器！
                        df_crypto['ma5'] = df_crypto['close'].rolling(5).mean()
                        df_crypto['ma10'] = df_crypto['close'].rolling(10).mean()
                        df_crypto['ma20'] = df_crypto['close'].rolling(20).mean()
                        df_crypto['ma60'] = df_crypto['close'].rolling(60).mean()
                        
                        # 扔掉前60天算不出均线的空数据
                        df_crypto = df_crypto.dropna(subset=['ma60']).reset_index(drop=True)
                        trend_result = self.trend_analyzer.analyze(df_crypto, code)
                else:
                    end_date = date.today()
                    start_date = end_date - timedelta(days=89)
                    historical_bars = self.db.get_data_range(code, start_date, end_date)
                    if historical_bars:
                        df = pd.DataFrame([bar.to_dict() for bar in historical_bars])
                        if self.config.enable_realtime_quote and realtime_quote:
                            df = self._augment_historical_with_realtime(df, realtime_quote, code)
                        trend_result = self.trend_analyzer.analyze(df, code)
            except Exception as e:
                logger.warning(f"趋势分析失败: {e}", exc_info=True)

            # Step 4: 情报搜索
            news_context = None
            if self.search_service.is_available:
                intel_results = self.search_service.search_comprehensive_intel(code, stock_name, 5)
                if intel_results:
                    news_context = self.search_service.format_intel_report(intel_results, stock_name)

            # Step 5 & 6: 获取与增强上下文
            context = self.db.get_analysis_context(code) or {'code': code, 'stock_name': stock_name, 'today': {}, 'yesterday': {}}
            enhanced_context = self._enhance_context(context, realtime_quote, chip_data, trend_result, stock_name)
            
            # Step 7: 调用 AI
            result = self.analyzer.analyze(enhanced_context, news_context=news_context)
            if result and realtime_quote:
                result.current_price = getattr(realtime_quote, 'price', None)
                result.change_pct = getattr(realtime_quote, 'change_pct', None)
            return result
        except Exception as e:
            logger.error(f"[{code}] 分析异常: {e}")
            return None
    
    def _enhance_context(self, context, realtime_quote, chip_data, trend_result, stock_name=""):
        enhanced = context.copy()
        
        # 🚀 注入恐慌贪婪指数
        if "-USD" in context.get('code', '').upper():
            enhanced['macro_sentiment_fng'] = self._get_fear_and_greed_index()

        if stock_name: enhanced['stock_name'] = stock_name
        elif realtime_quote and getattr(realtime_quote, 'name', None): enhanced['stock_name'] = realtime_quote.name
        
        if realtime_quote:
            enhanced['realtime'] = {
                'name': getattr(realtime_quote, 'name', ''),
                'price': getattr(realtime_quote, 'price', None),
                'change_pct': getattr(realtime_quote, 'change_pct', None),
                'volume_ratio': getattr(realtime_quote, 'volume_ratio', None),
                'turnover_rate': getattr(realtime_quote, 'turnover_rate', None),
            }
        
        if trend_result:
            enhanced['trend_analysis'] = {
                'trend_status': trend_result.trend_status.value,
                'ma_alignment': trend_result.ma_alignment,
                'bias_ma5': trend_result.bias_ma5,
                'buy_signal': trend_result.buy_signal.value,
            }

        if realtime_quote and trend_result and trend_result.ma5 > 0:
            price = getattr(realtime_quote, 'price', None)
            if price:
                enhanced['today'] = {
                    'close': price, 'ma5': trend_result.ma5, 
                    'ma10': trend_result.ma10, 'ma20': trend_result.ma20
                }
                enhanced['ma_status'] = self._compute_ma_status(price, trend_result.ma5, trend_result.ma10, trend_result.ma20)

        enhanced['is_index_etf'] = SearchService.is_index_or_etf(context.get('code', ''), enhanced.get('stock_name', stock_name))
        return enhanced

    def _analyze_with_agent(self, code, report_type, query_id, stock_name, realtime_quote, chip_data):
        try:
            from src.agent.factory import build_agent_executor
            executor = build_agent_executor(self.config, getattr(self.config, 'agent_skills', None) or None)
            fng_index = self._get_fear_and_greed_index() if "-USD" in code.upper() else None

            initial_context = {
                "stock_code": code, "stock_name": stock_name, "report_type": report_type.value,
                "realtime_quote": self._safe_to_dict(realtime_quote),
                "macro_sentiment_fng": fng_index
            }
            
            message = f"请分析标的 {code} ({stock_name})，并生成决策仪表盘报告。"
            agent_result = executor.run(message, context=initial_context)

            result = AnalysisResult(
                code=code, name=stock_name, sentiment_score=50, trend_prediction="未知",
                operation_advice="观望", success=agent_result.success,
            )
            if agent_result.success and agent_result.dashboard:
                result.dashboard = agent_result.dashboard
                result.sentiment_score = int(float(agent_result.dashboard.get("sentiment_score", 50)))
                result.operation_advice = agent_result.dashboard.get("operation_advice", "观望")
            if realtime_quote: result.current_price = getattr(realtime_quote, 'price', None)
            return result
        except Exception as e:
            logger.error(f"[{code}] Agent 分析失败: {e}")
            return None

    @staticmethod
    def _compute_ma_status(close, ma5, ma10, ma20) -> str:
        close, ma5, ma10, ma20 = close or 0, ma5 or 0, ma10 or 0, ma20 or 0
        if close > ma5 > ma10 > ma20 > 0: return "多头排列 📈"
        elif close < ma5 < ma10 < ma20 and ma20 > 0: return "空头排列 📉"
        elif close > ma5 and ma5 > ma10: return "短期向好 🔼"
        elif close < ma5 and ma5 < ma10: return "短期走弱 🔽"
        return "震荡整理 ↔️"

    def _augment_historical_with_realtime(self, df, realtime_quote, code):
        if df is None or df.empty or 'close' not in df.columns or realtime_quote is None: return df
        price = getattr(realtime_quote, 'price', None)
        if not price or price <= 0: return df
        
        market = get_market_for_stock(code)
        if market and not is_market_open(market, date.today()): return df

        last_date = df['date'].max()
        if pd.Timestamp(last_date).date() >= date.today():
            df.loc[df.index[-1], 'close'] = price
        else:
            new_row = {'code': code, 'date': date.today(), 'open': price, 'high': price, 'low': price, 'close': price, 'volume': 0}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        return df

    @staticmethod
    def _safe_to_dict(value: Any):
        if hasattr(value, "to_dict"): return value.to_dict()
        if hasattr(value, "__dict__"): return dict(value.__dict__)
        return None

    def process_single_stock(self, code, skip_analysis=False, single_stock_notify=False, report_type=ReportType.SIMPLE, analysis_query_id=None):
        try:
            self.fetch_and_save_stock_data(code)
            if skip_analysis: return None
            return self.analyze_stock(code, report_type, query_id=analysis_query_id or uuid.uuid4().hex)
        except Exception as e:
            return None
    
    def run(self, stock_codes=None, dry_run=False, send_notification=True, merge_notification=False):
        start_time = time.time()
        if stock_codes is None:
            self.config.refresh_stock_list()
            stock_codes = self.config.stock_list
            
        if not stock_codes: return[]
        
        if not dry_run: self.fetcher_manager.prefetch_stock_names(stock_codes, use_bulk=False)

        single_stock_notify = getattr(self.config, 'single_stock_notify', False)
        report_type_str = getattr(self.config, 'report_type', 'simple').lower()
        report_type = ReportType.FULL if report_type_str == 'full' else ReportType.SIMPLE
        
        results: List[AnalysisResult] =[]
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_code = {
                executor.submit(
                    self.process_single_stock, code, skip_analysis=dry_run,
                    single_stock_notify=single_stock_notify and send_notification,
                    report_type=report_type, analysis_query_id=uuid.uuid4().hex
                ): code for code in stock_codes
            }
            for idx, future in enumerate(as_completed(future_to_code)):
                try:
                    res = future.result()
                    if res: results.append(res)
                except Exception: pass
        
        if results and send_notification and not dry_run:
            if single_stock_notify or merge_notification:
                self._send_notifications(results, skip_push=True)
            else:
                self._send_notifications(results)
        
        return results

    def _send_notifications(self, results: List[AnalysisResult], skip_push: bool = False) -> None:
        try:
            report = self.notifier.generate_dashboard_report(results)
            self.notifier.save_report_to_file(report)
            
            if skip_push: return
            
            if self.notifier.is_available():
                self.notifier.send_to_context(report)
                
                channels = self.notifier.get_available_channels()
                wechat_success = False
                if NotificationChannel.WECHAT in channels:
                    wechat_report = self.notifier.generate_wechat_dashboard(results)
                    wechat_success = self.notifier.send_to_wechat(wechat_report)
                
                non_wechat_success = False
                for ch in channels:
                    if ch == NotificationChannel.WECHAT: continue
                    if ch == NotificationChannel.TELEGRAM:
                        non_wechat_success = self.notifier.send_to_telegram(report) or non_wechat_success
                    
                if wechat_success or non_wechat_success:
                    logger.info("推送成功")
                else:
                    logger.warning("推送失败")
        except Exception as e:
            logger.error(f"发送通知失败: {e}")
