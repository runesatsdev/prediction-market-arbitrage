#!/usr/bin/env python3

"""
Prediction Market Arbitrage Bot - FULL MARKET SCANNER WITH DIAGNOSTICS

Based on IMDEA Networks research: $39.59M arbitrage extraction (Apr 2024-Apr 2025)

Strategies Implemented:
1. Single-Condition Arbitrage (YES + NO ≠ $1.00) - $10.58M extracted
2. NegRisk Rebalancing (Σ(prices) ≠ 1.00) - $28.99M extracted (29× capital efficiency)
3. Whale Tracking - Follow informed traders

FREE Data Sources:
- Polymarket CLOB API (REST)
- Gamma Markets API (backup)
- Public market data, no auth required
"""

import asyncio
import aiohttp
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import logging
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('arbitrage_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ArbitrageOpportunity:
    """Represents a detected arbitrage opportunity"""
    market_id: str
    market_name: str
    opportunity_type: str  # 'single_condition', 'negrisk', 'whale'
    expected_profit: float
    roi: float
    capital_required: float
    risk_score: float
    urgency: str  # 'high', 'medium', 'low'
    details: Dict
    timestamp: datetime


class PolymarketClient:
    """Free Polymarket API client - multiple endpoints for reliability"""

    # Multiple API endpoints for redundancy
    CLOB_URL = "https://clob.polymarket.com"
    GAMMA_URL = "https://gamma-api.polymarket.com"
    STRAPI_URL = "https://strapi-matic.poly.market"

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.markets_cache = {}
        self.orderbook_cache = defaultdict(lambda: deque(maxlen=100))
        self.diagnostics = {
            'markets_fetched': 0,
            'markets_with_tokens': 0,
            'orderbooks_fetched': 0,
            'orderbooks_with_data': 0,
            'markets_analyzed': 0
        }

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_all_markets(self) -> List[Dict]:
        """Fetch ALL active markets using pagination"""

        all_markets = []

        # Try multiple strategies to get maximum markets
        logger.info("🔍 Fetching ALL available markets...")

        # Strategy 1: CLOB sampling-markets (most reliable with proper token structure)
        clob_markets = await self._fetch_from_clob_paginated()
        if clob_markets:
            all_markets.extend(clob_markets)
            logger.info(f"✓ Fetched {len(clob_markets)} markets from CLOB API")

        # Strategy 2: Gamma API as backup
        if len(all_markets) < 100:
            gamma_markets = await self._fetch_from_gamma_paginated()
            if gamma_markets:
                # Deduplicate by market ID
                existing_ids = {m.get('condition_id') or m.get('id') or m.get('market_id') for m in all_markets}
                new_markets = [m for m in gamma_markets
                              if (m.get('condition_id') or m.get('id') or m.get('market_id')) not in existing_ids]
                all_markets.extend(new_markets)
                logger.info(f"✓ Fetched {len(gamma_markets)} markets from Gamma API ({len(new_markets)} new)")

        if not all_markets:
            logger.warning("⚠️  Could not fetch markets from any endpoint")
            return []

        # Enrich markets with full details from CLOB API to get token_ids
        logger.info("🔧 Enriching markets with CLOB details...")
        enriched_markets = []

        max_markets = min(len(all_markets), 200)  # Process up to 200 markets per scan
        for i, market in enumerate(all_markets[:max_markets]):
            condition_id = market.get('condition_id') or market.get('conditionId')

            if condition_id and condition_id.strip():  # Only if we have a valid condition_id
                # Fetch full market details from CLOB
                full_market = await self._fetch_market_from_clob(condition_id)
                if full_market:
                    enriched_markets.append(full_market)
                else:
                    enriched_markets.append(market)  # Fallback to original

                if (i + 1) % 50 == 0:
                    logger.info(f"  Enriched {i + 1}/{max_markets} markets...")

                await asyncio.sleep(0.05)  # Rate limiting
            else:
                enriched_markets.append(market)

        # Cache all markets
        for market in enriched_markets:
            market_id = market.get('id') or market.get('condition_id') or market.get('market_id')
            if market_id:
                self.markets_cache[market_id] = market

        self.diagnostics['markets_fetched'] = len(enriched_markets)
        logger.info(f"📊 TOTAL MARKETS LOADED: {len(enriched_markets)}")
        return enriched_markets

    async def _fetch_from_gamma_paginated(self) -> List[Dict]:
        """Fetch from Gamma API with pagination"""
        all_markets = []

        try:
            # Fetch in batches
            for offset in range(0, 1000, 100):  # Try up to 1000 markets
                url = f"{self.GAMMA_URL}/markets"
                params = {
                    'limit': 100,
                    'offset': offset,
                    'active': 'true',
                    'closed': 'false'
                }

                headers = {
                    'User-Agent': 'Mozilla/5.0 (compatible; ArbitrageBot/1.0)',
                }

                async with self.session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()

                        # Handle different response formats
                        if isinstance(data, list):
                            markets = data
                        elif isinstance(data, dict) and 'data' in data:
                            markets = data['data']
                        elif isinstance(data, dict) and 'markets' in data:
                            markets = data['markets']
                        else:
                            markets = []

                        if not markets:
                            break  # No more markets

                        all_markets.extend(markets)

                        if len(markets) < 100:
                            break  # Last page

                        await asyncio.sleep(0.2)  # Rate limiting
                    else:
                        break

            return all_markets

        except Exception as e:
            logger.debug(f"Gamma API pagination error: {e}")
            return all_markets

    async def _fetch_from_clob_paginated(self) -> List[Dict]:
        """Fetch from CLOB API with pagination"""
        all_markets = []

        try:
            # Try sampling-markets endpoint which is more reliable
            url = f"{self.CLOB_URL}/sampling-markets"
            params = {'limit': 500}

            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; ArbitrageBot/1.0)',
            }

            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    markets = data if isinstance(data, list) else []
                    all_markets.extend(markets)

            return all_markets

        except Exception as e:
            logger.debug(f"CLOB API pagination error: {e}")
            return all_markets

    async def _fetch_market_from_clob(self, condition_id: str) -> Optional[Dict]:
        """Fetch full market details from CLOB API by condition_id"""
        try:
            url = f"{self.CLOB_URL}/markets/{condition_id}"

            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; ArbitrageBot/1.0)',
            }

            async with self.session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    market = await resp.json()
                    return market
                else:
                    logger.debug(f"CLOB market fetch returned {resp.status} for {condition_id}")
                    return None

        except Exception as e:
            logger.debug(f"Error fetching market from CLOB: {e}")
            return None

    async def get_orderbook(self, token_id: str) -> Optional[Dict]:
        """Get orderbook for a specific outcome token"""
        if not token_id:
            return None

        try:
            url = f"{self.CLOB_URL}/book"
            params = {'token_id': token_id}

            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; ArbitrageBot/1.0)',
            }

            async with self.session.get(url, params=params, headers=headers) as resp:
                self.diagnostics['orderbooks_fetched'] += 1

                if resp.status == 200:
                    book = await resp.json()

                    # Check if orderbook has actual data
                    if book and (book.get('asks') or book.get('bids')):
                        self.diagnostics['orderbooks_with_data'] += 1

                    return book
                else:
                    # Log non-200 responses for debugging
                    if self.diagnostics['orderbooks_fetched'] <= 5:
                        text = await resp.text()
                        logger.info(f"❌ Orderbook API returned {resp.status} for token {token_id}: {text[:200]}")
                    return None
        except Exception as e:
            if self.diagnostics['orderbooks_fetched'] <= 5:
                logger.info(f"❌ Exception fetching orderbook for {token_id}: {e}")
            return None

    async def get_market_trades(self, market_id: str, limit: int = 100) -> List[Dict]:
        """Get recent trades for whale tracking"""
        if not market_id:
            return []

        try:
            # Try CLOB trades endpoint
            url = f"{self.CLOB_URL}/trades"
            params = {'market': market_id, 'limit': limit}

            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; ArbitrageBot/1.0)',
            }

            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    trades = await resp.json()
                    return trades if isinstance(trades, list) else []
                return []
        except Exception as e:
            logger.debug(f"Error fetching trades: {e}")
            return []


class ArbitrageDetector:
    """Implements arbitrage detection strategies from IMDEA research"""

    # Research-backed thresholds - Based on 2024-2025 market analysis
    # Real arbitrage found: 1-2.5% typical, up to 18% exceptional
    MIN_PROFIT_THRESHOLD = 0.005  # 0.5 cents minimum (captures 0.5%+ spreads)
    MAX_PROFIT_THRESHOLD = 0.50  # 50 cents max (filter out stale markets)
    MIN_LIQUIDITY = 5.0  # $5 minimum for realistic trading
    NEGRISK_MULTIPLIER = 29  # 29× capital efficiency advantage
    WHALE_THRESHOLD = 1000  # $1,000+ trades
    HIGH_URGENCY_ROI = 0.10  # 10%+ ROI (research shows this is common)
    MEDIUM_URGENCY_ROI = 0.025  # 2.5%+ ROI (typical range)

    def __init__(self):
        self.opportunities: List[ArbitrageOpportunity] = []
        self.whale_addresses = set()
        self.diagnostics = {
            'single_condition_checked': 0,
            'single_condition_found': 0,
            'negrisk_checked': 0,
            'negrisk_found': 0,
            'whale_checked': 0,
            'whale_found': 0,
            'no_orderbook': 0,
            'no_prices': 0
        }

    def detect_single_condition_arbitrage(
        self,
        market: Dict,
        yes_orderbook: Optional[Dict],
        no_orderbook: Optional[Dict]
    ) -> Optional[ArbitrageOpportunity]:
        """
        Strategy 1: YES + NO ≠ $1.00
        IMDEA Research: $10.58M extracted, 7,051 conditions
        """
        self.diagnostics['single_condition_checked'] += 1

        if not yes_orderbook or not no_orderbook:
            self.diagnostics['no_orderbook'] += 1
            return None

        try:
            # Filter out closed/archived markets
            if market.get('closed') or market.get('archived') or not market.get('active'):
                return None

            # Get best prices
            yes_asks = yes_orderbook.get('asks', [])
            no_asks = no_orderbook.get('asks', [])

            if not yes_asks or not no_asks:
                self.diagnostics['no_prices'] += 1
                return None

            yes_best_ask = float(yes_asks[0].get('price', 0))
            no_best_ask = float(no_asks[0].get('price', 0))

            if yes_best_ask == 0 or no_best_ask == 0:
                self.diagnostics['no_prices'] += 1
                return None

            # Filter out placeholder/extreme prices (likely inactive markets)
            # Real markets rarely have both sides at 0.95+
            if yes_best_ask >= 0.95 and no_best_ask >= 0.95:
                return None

            sum_price = yes_best_ask + no_best_ask
            deviation = abs(1.0 - sum_price)

            # Filter out unrealistic deviations (>50% = likely stale/closed market)
            if deviation > 0.50:
                return None

            # Log ALL deviations for debugging
            if deviation > 0:
                logger.debug(f"Price deviation found: {deviation:.4f} (sum={sum_price:.4f})")

            # Check if profitable
            if deviation > self.MIN_PROFIT_THRESHOLD:
                # Get liquidity
                yes_liquidity = sum(float(ask.get('size', 0)) for ask in yes_asks[:5])
                no_liquidity = sum(float(ask.get('size', 0)) for ask in no_asks[:5])
                min_liquidity = min(yes_liquidity, no_liquidity)

                if min_liquidity < self.MIN_LIQUIDITY:
                    return None

                capital_required = sum_price * min_liquidity
                expected_profit = deviation * min_liquidity
                roi = deviation / sum_price if sum_price > 0 else 0

                # Risk scoring
                risk_score = self._calculate_risk_score(market, 'single_condition')

                # Urgency classification
                urgency = 'high' if roi > self.HIGH_URGENCY_ROI else 'medium' if roi > self.MEDIUM_URGENCY_ROI else 'low'

                # Get market name
                market_name = (market.get('question') or
                             market.get('title') or
                             market.get('description') or
                             'Unknown Market')[:80]

                market_id = (market.get('condition_id') or
                           market.get('id') or
                           market.get('market_id') or
                           'unknown')

                self.diagnostics['single_condition_found'] += 1

                return ArbitrageOpportunity(
                    market_id=str(market_id),
                    market_name=market_name,
                    opportunity_type='single_condition',
                    expected_profit=expected_profit,
                    roi=roi,
                    capital_required=capital_required,
                    risk_score=risk_score,
                    urgency=urgency,
                    details={
                        'yes_price': yes_best_ask,
                        'no_price': no_best_ask,
                        'sum_price': sum_price,
                        'deviation': deviation,
                        'liquidity': min_liquidity,
                        'action': 'buy_both' if sum_price < 1.0 else 'sell_both'
                    },
                    timestamp=datetime.now()
                )
        except Exception as e:
            logger.debug(f"Error in single-condition detection: {e}")

        return None

    def detect_negrisk_arbitrage(
        self,
        market: Dict,
        orderbooks: Dict[str, Dict]
    ) -> Optional[ArbitrageOpportunity]:
        """
        Strategy 2: NegRisk Rebalancing (Σ prices ≠ 1.0 across N≥3 conditions)
        IMDEA Research: $28.99M extracted, 662 markets, 29× capital efficiency
        """
        self.diagnostics['negrisk_checked'] += 1

        # Filter out closed/archived markets
        if market.get('closed') or market.get('archived') or not market.get('active'):
            return None

        # Get tokens from market
        tokens = (market.get('tokens') or
                 market.get('outcomes') or
                 market.get('options') or [])

        # Only NegRisk markets (N≥3 mutually exclusive outcomes)
        if len(tokens) < 3:
            return None

        try:
            prices = []
            liquidities = []

            for token in tokens:
                # Extract token_id from dict
                if isinstance(token, dict):
                    token_id = token.get('token_id') or token.get('id')
                else:
                    return None  # Can't process string tokens

                if not token_id or token_id not in orderbooks:
                    return None

                book = orderbooks[token_id]
                if not book:
                    return None

                asks = book.get('asks', [])
                if not asks:
                    return None

                best_ask = float(asks[0].get('price', 0))

                if best_ask == 0:
                    return None

                prices.append(best_ask)

                # Calculate liquidity
                liquidity = sum(float(ask.get('size', 0)) for ask in asks[:5])
                liquidities.append(liquidity)

            # Check probability sum deviation
            prob_sum = sum(prices)
            deviation = abs(1.0 - prob_sum)

            # Log ALL deviations for debugging
            if deviation > 0:
                logger.debug(f"NegRisk deviation found: {deviation:.4f} (sum={prob_sum:.4f})")

            # Filter out unrealistic deviations (>50% = likely stale/closed market)
            if deviation > self.MAX_PROFIT_THRESHOLD:
                return None

            # Check if profitable
            if deviation > self.MIN_PROFIT_THRESHOLD:
                min_liquidity = min(liquidities)

                if min_liquidity < self.MIN_LIQUIDITY:
                    return None

                capital_required = prob_sum * min_liquidity
                expected_profit = deviation * min_liquidity

                # Apply 29× capital efficiency multiplier from research
                effective_roi = (deviation / prob_sum) * self.NEGRISK_MULTIPLIER if prob_sum > 0 else 0
                roi = deviation / prob_sum if prob_sum > 0 else 0

                risk_score = self._calculate_risk_score(market, 'negrisk')
                urgency = 'high' if effective_roi > self.HIGH_URGENCY_ROI else 'medium'

                market_name = (market.get('question') or
                             market.get('title') or
                             market.get('description') or
                             'Unknown Market')[:80]

                market_id = (market.get('condition_id') or
                           market.get('id') or
                           market.get('market_id') or
                           'unknown')

                self.diagnostics['negrisk_found'] += 1

                return ArbitrageOpportunity(
                    market_id=str(market_id),
                    market_name=market_name,
                    opportunity_type='negrisk',
                    expected_profit=expected_profit,
                    roi=roi,
                    capital_required=capital_required,
                    risk_score=risk_score,
                    urgency=urgency,
                    details={
                        'num_conditions': len(tokens),
                        'prices': [f"{p:.4f}" for p in prices],
                        'prob_sum': prob_sum,
                        'deviation': deviation,
                        'min_liquidity': min_liquidity,
                        'capital_efficiency': f'{self.NEGRISK_MULTIPLIER}×',
                        'action': 'buy_all' if prob_sum < 1.0 else 'sell_all'
                    },
                    timestamp=datetime.now()
                )
        except Exception as e:
            logger.debug(f"Error in NegRisk detection: {e}")

        return None

    def _calculate_risk_score(self, market: Dict, strategy_type: str) -> float:
        """
        Calculate risk score (0-1, lower is better)
        Factors: Resolution date, liquidity, oracle risk
        """
        try:
            risk = 0.0

            # Time to resolution risk
            end_date_str = (market.get('end_date_iso') or
                          market.get('end_date') or
                          market.get('close_time'))

            if end_date_str:
                try:
                    end_date = datetime.fromisoformat(str(end_date_str).replace('Z', '+00:00'))
                    days_to_resolution = (end_date - datetime.now()).days

                    # Higher risk near resolution (oracle manipulation)
                    if days_to_resolution < 2:
                        risk += 0.4
                    elif days_to_resolution < 7:
                        risk += 0.2
                except:
                    pass

            # Strategy-specific risks
            if strategy_type == 'negrisk':
                # More complex execution = more risk
                num_tokens = len(market.get('tokens', []))
                risk += min(0.2, num_tokens * 0.03)

            # Subjective oracle risk
            question = (market.get('question') or
                       market.get('title') or
                       market.get('description') or '').lower()

            subjective_keywords = ['best', 'winner', 'better', 'more popular', 'succeed', 'who will']
            if any(keyword in question for keyword in subjective_keywords):
                risk += 0.3

            return min(1.0, risk)

        except Exception as e:
            logger.debug(f"Error calculating risk: {e}")
            return 0.5  # Default medium risk


class AlertManager:
    """Manage and display opportunities"""

    def __init__(self):
        self.displayed_opportunities = set()

    def display_opportunity(self, opp: ArbitrageOpportunity):
        """Display opportunity in formatted way"""

        # Avoid duplicate alerts (within 5 minutes)
        opp_key = f"{opp.market_id}_{opp.opportunity_type}"
        if opp_key in self.displayed_opportunities:
            return
        self.displayed_opportunities.add(opp_key)

        # Color coding
        urgency_symbol = "🔴" if opp.urgency == 'high' else "🟡" if opp.urgency == 'medium' else "🟢"

        print("\n" + "="*80)
        print(f"{urgency_symbol} ARBITRAGE OPPORTUNITY DETECTED - {opp.opportunity_type.upper()}")
        print("="*80)
        print(f"Market: {opp.market_name}")
        print(f"Expected Profit: ${opp.expected_profit:.2f}")
        print(f"ROI: {opp.roi*100:.2f}%")
        print(f"Capital Required: ${opp.capital_required:.2f}")
        print(f"Risk Score: {opp.risk_score:.2f}/1.00")
        print(f"Urgency: {opp.urgency.upper()}")
        print(f"\nDetails:")
        for key, value in opp.details.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.4f}")
            else:
                print(f"  {key}: {value}")
        print(f"\nTimestamp: {opp.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")

        logger.info(f"OPPORTUNITY: {opp.opportunity_type} - ${opp.expected_profit:.2f} profit, {opp.roi*100:.1f}% ROI")

    def generate_summary(self, opportunities: List[ArbitrageOpportunity], client_diag: Dict, detector_diag: Dict):
        """Generate summary statistics with diagnostics"""

        print("\n" + "="*80)
        print("📊 SCAN DIAGNOSTICS")
        print("="*80)
        print(f"Markets fetched: {client_diag['markets_fetched']}")
        print(f"Markets with tokens: {client_diag['markets_with_tokens']}")
        print(f"Orderbooks fetched: {client_diag['orderbooks_fetched']}")
        print(f"Orderbooks with data: {client_diag['orderbooks_with_data']}")
        print(f"\nSingle-Condition markets checked: {detector_diag['single_condition_checked']}")
        print(f"Single-Condition opportunities found: {detector_diag['single_condition_found']}")
        print(f"NegRisk markets checked: {detector_diag['negrisk_checked']}")
        print(f"NegRisk opportunities found: {detector_diag['negrisk_found']}")
        print(f"\nMarkets with no orderbook: {detector_diag['no_orderbook']}")
        print(f"Markets with no prices: {detector_diag['no_prices']}")
        print("="*80)

        if not opportunities:
            print("\n📊 No opportunities detected in this scan.\n")
            return

        total_profit = sum(opp.expected_profit for opp in opportunities)
        total_capital = sum(opp.capital_required for opp in opportunities)
        avg_roi = np.mean([opp.roi for opp in opportunities]) * 100

        by_type = defaultdict(list)
        for opp in opportunities:
            by_type[opp.opportunity_type].append(opp)

        print("\n" + "="*80)
        print("📊 OPPORTUNITIES SUMMARY")
        print("="*80)
        print(f"Total Opportunities: {len(opportunities)}")
        print(f"Total Expected Profit: ${total_profit:.2f}")
        print(f"Total Capital Required: ${total_capital:.2f}")
        print(f"Average ROI: {avg_roi:.2f}%")
        print(f"\nBy Strategy:")
        for strategy, opps in by_type.items():
            strategy_profit = sum(o.expected_profit for o in opps)
            print(f"  {strategy}: {len(opps)} opportunities, ${strategy_profit:.2f} profit")
        print("="*80 + "\n")


class PredictionMarketBot:
    """Main bot orchestrator - FULL MARKET SCANNER"""

    def __init__(self, scan_interval: int = 120):
        self.scan_interval = scan_interval
        self.detector = ArbitrageDetector()
        self.alert_manager = AlertManager()
        self.scan_count = 0

    async def analyze_market_batch(self, markets: List[Dict], client: PolymarketClient,
                                   batch_num: int, total_batches: int) -> List[ArbitrageOpportunity]:
        """Analyze a batch of markets concurrently"""
        opportunities = []

        logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(markets)} markets)")

        for i, market in enumerate(markets):
            try:
                market_name = (market.get('question') or
                             market.get('title') or
                             market.get('description') or
                             'Unknown')[:60]

                # Get tokens - CLOB API returns 'tokens' array with proper structure
                tokens = market.get('tokens') or []

                # Parse if it's a JSON string (from Gamma API)
                if isinstance(tokens, str):
                    try:
                        tokens = json.loads(tokens)
                    except:
                        tokens = []

                # If tokens is still empty, try clob_token_ids from Gamma API
                if not tokens:
                    clob_token_ids = market.get('clob_token_ids')
                    outcomes_str = market.get('outcomes') or market.get('options') or []

                    if clob_token_ids and outcomes_str:
                        # Parse outcomes
                        if isinstance(outcomes_str, str):
                            try:
                                outcomes_list = json.loads(outcomes_str)
                            except:
                                outcomes_list = []
                        else:
                            outcomes_list = outcomes_str

                        # Parse clob_token_ids
                        if isinstance(clob_token_ids, str):
                            try:
                                token_ids_list = json.loads(clob_token_ids)
                            except:
                                token_ids_list = []
                        else:
                            token_ids_list = clob_token_ids if isinstance(clob_token_ids, list) else []

                        # Match outcomes with token_ids
                        if len(token_ids_list) == len(outcomes_list):
                            tokens = []
                            for outcome, token_id in zip(outcomes_list, token_ids_list):
                                tokens.append({
                                    'outcome': outcome,
                                    'token_id': token_id,
                                    'price': 0.5  # Default, will be updated from orderbook
                                })

                if tokens and isinstance(tokens, list):
                    client.diagnostics['markets_with_tokens'] += 1

                if not tokens or not isinstance(tokens, list):
                    continue

                # Log first market with tokens for debugging
                if client.diagnostics['markets_with_tokens'] == 1:
                    logger.info(f"📍 Sample market keys: {list(market.keys())}")
                    logger.info(f"📍 Tokens found: {json.dumps(tokens, indent=2)}")

                # Fetch orderbooks for all tokens
                orderbooks = {}
                for token in tokens[:10]:  # Limit to 10 tokens max
                    # Handle different token formats
                    if isinstance(token, dict):
                        # CLOB API format: {token_id: "...", outcome: "Yes", ...}
                        token_id = token.get('token_id') or token.get('id')
                    elif isinstance(token, str):
                        # Gamma API format: just strings like "Yes", "No" - we can't fetch orderbooks
                        token_id = None
                    else:
                        token_id = None

                    # Debug: Log what token_id we're trying to use
                    if client.diagnostics['markets_with_tokens'] <= 3:
                        logger.info(f"🔎 Token: {token}")
                        logger.info(f"🔎 Extracted token_id: {token_id}")

                    if token_id:
                        book = await client.get_orderbook(token_id)
                        if book:
                            orderbooks[token_id] = book
                            # Log first orderbook for debugging
                            if client.diagnostics['orderbooks_with_data'] == 1:
                                logger.info(f"✅ Got orderbook! Structure: {json.dumps(book, indent=2)[:500]}")
                        else:
                            if client.diagnostics['markets_with_tokens'] <= 3:
                                logger.info(f"❌ Failed to fetch orderbook for token_id: {token_id}")
                        await asyncio.sleep(0.05)  # Rate limiting
                    else:
                        if client.diagnostics['markets_with_tokens'] <= 3:
                            logger.info(f"⚠️  No token_id found for token: {token}")

                # Detect Single-Condition Arbitrage
                if len(tokens) == 2:
                    # Extract token IDs
                    token1_id = None
                    token2_id = None

                    if isinstance(tokens[0], dict):
                        token1_id = tokens[0].get('token_id') or tokens[0].get('id')
                    if isinstance(tokens[1], dict):
                        token2_id = tokens[1].get('token_id') or tokens[1].get('id')

                    if token1_id and token2_id:
                        opp = self.detector.detect_single_condition_arbitrage(
                            market,
                            orderbooks.get(token1_id),
                            orderbooks.get(token2_id)
                        )

                        if opp:
                            opportunities.append(opp)
                            self.alert_manager.display_opportunity(opp)

                # Detect NegRisk Arbitrage
                elif len(tokens) >= 3:
                    opp = self.detector.detect_negrisk_arbitrage(market, orderbooks)

                    if opp:
                        opportunities.append(opp)
                        self.alert_manager.display_opportunity(opp)

                await asyncio.sleep(0.1)  # Rate limiting

                client.diagnostics['markets_analyzed'] += 1

            except Exception as e:
                logger.debug(f"  ⚠️  Error analyzing market: {e}")
                continue

        return opportunities

    async def run_single_scan(self):
        """Run one complete scan cycle - FULL MARKET"""
        self.scan_count += 1
        logger.info(f"\n{'='*80}")
        logger.info(f"🔍 Starting FULL MARKET Scan #{self.scan_count}")
        logger.info(f"{'='*80}\n")

        async with PolymarketClient() as client:
            # Fetch ALL markets
            all_markets = await client.get_all_markets()

            if not all_markets:
                logger.warning("⚠️  No markets fetched. This could mean:")
                logger.warning("   1. Polymarket API is temporarily down")
                logger.warning("   2. Network connectivity issues")
                logger.warning("   3. API rate limiting")
                logger.warning("   → Will retry next cycle...")
                return

            logger.info(f"🎯 Analyzing {len(all_markets)} markets for arbitrage...\n")

            # Process in batches to manage memory and provide progress updates
            batch_size = 50
            all_opportunities = []
            total_batches = (len(all_markets) + batch_size - 1) // batch_size

            for i in range(0, len(all_markets), batch_size):
                batch = all_markets[i:i+batch_size]
                batch_num = (i // batch_size) + 1

                batch_opps = await self.analyze_market_batch(
                    batch, client, batch_num, total_batches
                )
                all_opportunities.extend(batch_opps)

                # Small delay between batches
                await asyncio.sleep(0.5)

            # Generate summary with diagnostics
            self.alert_manager.generate_summary(all_opportunities, client.diagnostics, self.detector.diagnostics)

            logger.info(f"✓ Scan #{self.scan_count} complete. Found {len(all_opportunities)} opportunities.")
            logger.info(f"⏰ Next scan in {self.scan_interval} seconds...\n")

    async def run_continuous(self):
        """Run continuous monitoring - FULL MARKET"""
        logger.info("🚀 Prediction Market Arbitrage Bot Starting (FULL MARKET MODE)...")
        logger.info(f"📊 Scanning up to 200 markets per cycle")
        logger.info(f"⏰ Scan interval: {self.scan_interval} seconds")
        logger.info(f"💰 Profit threshold: ${self.detector.MIN_PROFIT_THRESHOLD*100:.1f}¢ to ${self.detector.MAX_PROFIT_THRESHOLD*100:.1f}¢")
        logger.info("\nStrategies Active:")
        logger.info("  1. Single-Condition Arbitrage (YES+NO≠$1.00)")
        logger.info("  2. NegRisk Rebalancing (Σprices≠1.00, 29× efficiency)")
        logger.info("\n" + "="*80 + "\n")

        while True:
            try:
                await self.run_single_scan()
                await asyncio.sleep(self.scan_interval)
            except KeyboardInterrupt:
                logger.info("\n\n🛑 Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"⚠️  Error in main loop: {e}")
                logger.info(f"   Retrying in {self.scan_interval} seconds...")
                await asyncio.sleep(self.scan_interval)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

async def main():
    """Main entry point"""

    print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                  PREDICTION MARKET ARBITRAGE BOT                          ║
║                        FULL MARKET SCANNER                                ║
║                                                                           ║
║  Based on IMDEA Networks Research: $39.59M Arbitrage Extracted           ║
║  April 2024 - April 2025                                                 ║
║                                                                           ║
║  Strategies:                                                             ║
║    • Single-Condition: $10.58M extracted (7,051 conditions)              ║
║    • NegRisk: $28.99M extracted (662 markets, 29× efficiency)            ║
║                                                                           ║
║  ✅ Filters: Active markets only, realistic spreads (1-50¢)              ║
║  ✅ Scans: Up to 200 markets every 2 minutes                             ║
║                                                                           ║
║  ⚠️  DISCLAIMER: Detection only - NOT automatic execution                 ║
║  ⚠️  Always verify opportunities manually before trading                  ║
║  ⚠️  Prediction markets involve significant risk                          ║
╚═══════════════════════════════════════════════════════════════════════════╝
    """)

    # Configuration
    SCAN_INTERVAL = 120  # 2 minutes between full market scans

    bot = PredictionMarketBot(
        scan_interval=SCAN_INTERVAL
    )

    try:
        await bot.run_continuous()
    except KeyboardInterrupt:
        print("\n\n✋ Shutting down gracefully...")
        logger.info("Bot shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
