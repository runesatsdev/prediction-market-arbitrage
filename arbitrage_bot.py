#!/usr/bin/env python3

"""
Prediction Market Arbitrage Bot - FULL MARKET SCANNER

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

        # Strategy 1: Gamma API with high limit
        markets = await self._fetch_from_gamma_paginated()
        if markets:
            all_markets.extend(markets)
            logger.info(f"✓ Fetched {len(markets)} markets from Gamma API")

        # Strategy 2: CLOB API with pagination
        clob_markets = await self._fetch_from_clob_paginated()
        if clob_markets:
            # Deduplicate by market ID
            existing_ids = {m.get('condition_id') or m.get('id') or m.get('market_id') for m in all_markets}
            new_markets = [m for m in clob_markets
                          if (m.get('condition_id') or m.get('id') or m.get('market_id')) not in existing_ids]
            all_markets.extend(new_markets)
            logger.info(f"✓ Fetched {len(clob_markets)} markets from CLOB API ({len(new_markets)} new)")

        # Strategy 3: Strapi API (market metadata)
        strapi_markets = await self._fetch_from_strapi()
        if strapi_markets:
            existing_ids = {m.get('condition_id') or m.get('id') or m.get('market_id') for m in all_markets}
            new_markets = [m for m in strapi_markets
                          if (m.get('condition_id') or m.get('id') or m.get('market_id')) not in existing_ids]
            all_markets.extend(new_markets)
            logger.info(f"✓ Fetched {len(strapi_markets)} markets from Strapi API ({len(new_markets)} new)")

        if not all_markets:
            logger.warning("⚠️  Could not fetch markets from any endpoint")
            return []

        # Cache all markets
        for market in all_markets:
            market_id = market.get('id') or market.get('condition_id') or market.get('market_id')
            if market_id:
                self.markets_cache[market_id] = market

        logger.info(f"📊 TOTAL MARKETS LOADED: {len(all_markets)}")
        return all_markets

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
            # Try different endpoints
            endpoints = [
                '/sampling-markets',
                '/markets',
            ]

            for endpoint in endpoints:
                for offset in range(0, 1000, 100):
                    url = f"{self.CLOB_URL}{endpoint}"
                    params = {
                        'limit': 100,
                        'offset': offset
                    }

                    headers = {
                        'User-Agent': 'Mozilla/5.0 (compatible; ArbitrageBot/1.0)',
                    }

                    async with self.session.get(url, params=params, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            markets = data if isinstance(data, list) else []

                            if not markets:
                                break

                            all_markets.extend(markets)

                            if len(markets) < 100:
                                break

                            await asyncio.sleep(0.2)
                        else:
                            break

                if all_markets:
                    break  # Got markets from this endpoint

            return all_markets

        except Exception as e:
            logger.debug(f"CLOB API pagination error: {e}")
            return all_markets

    async def _fetch_from_strapi(self) -> List[Dict]:
        """Fetch from Strapi API (market metadata)"""
        try:
            url = f"{self.STRAPI_URL}/markets"
            params = {
                'pagination[limit]': 500,
                'filters[closed][$eq]': 'false',
                'filters[active][$eq]': 'true'
            }

            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; ArbitrageBot/1.0)',
            }

            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()

                    if isinstance(data, dict) and 'data' in data:
                        markets = data['data']
                        # Convert Strapi format to standard format
                        return [m.get('attributes', m) for m in markets if isinstance(m, dict)]

                return []

        except Exception as e:
            logger.debug(f"Strapi API error: {e}")
            return []

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
                if resp.status == 200:
                    book = await resp.json()
                    return book
                return None
        except Exception as e:
            logger.debug(f"Error fetching orderbook for {token_id}: {e}")
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

    # Research-backed thresholds
    MIN_PROFIT_THRESHOLD = 0.015  # 1.5 cents minimum (lowered to catch more opportunities)
    NEGRISK_MULTIPLIER = 29  # 29× capital efficiency advantage
    WHALE_THRESHOLD = 5000  # $5,000+ trades
    HIGH_URGENCY_ROI = 0.08  # 8%+ ROI (lowered threshold)
    MEDIUM_URGENCY_ROI = 0.03  # 3%+ ROI (lowered threshold)

    def __init__(self):
        self.opportunities: List[ArbitrageOpportunity] = []
        self.whale_addresses = set()

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
        if not yes_orderbook or not no_orderbook:
            return None

        try:
            # Get best prices
            yes_asks = yes_orderbook.get('asks', [])
            no_asks = no_orderbook.get('asks', [])

            if not yes_asks or not no_asks:
                return None

            yes_best_ask = float(yes_asks[0].get('price', 0))
            no_best_ask = float(no_asks[0].get('price', 0))

            if yes_best_ask == 0 or no_best_ask == 0:
                return None

            sum_price = yes_best_ask + no_best_ask
            deviation = abs(1.0 - sum_price)

            # Check if profitable
            if deviation > self.MIN_PROFIT_THRESHOLD:
                # Get liquidity
                yes_liquidity = sum(float(ask.get('size', 0)) for ask in yes_asks[:5])
                no_liquidity = sum(float(ask.get('size', 0)) for ask in no_asks[:5])
                min_liquidity = min(yes_liquidity, no_liquidity)

                if min_liquidity < 5:  # Lowered minimum liquidity
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
                token_id = token.get('token_id') or token.get('id')
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

            # Check if profitable
            if deviation > self.MIN_PROFIT_THRESHOLD:
                min_liquidity = min(liquidities)

                if min_liquidity < 5:  # Lowered minimum liquidity
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

    def detect_whale_activity(
        self,
        market: Dict,
        recent_trades: List[Dict]
    ) -> Optional[ArbitrageOpportunity]:
        """
        Strategy 3: Whale Tracking
        Research: Top performer made $2.01M with 11 trades/day
        Whale signals predict 61.7-68.3% accuracy at T+15 to T+60 minutes
        """
        if not recent_trades:
            return None

        try:
            # Filter for whale-sized trades (>$5K)
            whale_trades = []

            for trade in recent_trades[-50:]:  # Last 50 trades
                size = float(trade.get('size', 0))
                price = float(trade.get('price', 0))
                trade_value = size * price

                if trade_value >= self.WHALE_THRESHOLD:
                    whale_trades.append({
                        'trader': trade.get('maker_address', 'unknown'),
                        'side': trade.get('side', 'unknown'),
                        'size': size,
                        'price': price,
                        'value': trade_value,
                        'timestamp': trade.get('timestamp', 0)
                    })

            if not whale_trades:
                return None

            # Analyze whale flow
            recent_whale = whale_trades[-1]
            total_whale_volume = sum(t['value'] for t in whale_trades)

            # Calculate directional imbalance
            buy_volume = sum(t['value'] for t in whale_trades if t['side'] == 'BUY')
            sell_volume = sum(t['value'] for t in whale_trades if t['side'] == 'SELL')
            flow_imbalance = (buy_volume - sell_volume) / (buy_volume + sell_volume) if (buy_volume + sell_volume) > 0 else 0

            # Strong signal if imbalance > 40%
            if abs(flow_imbalance) > 0.4:
                expected_profit = total_whale_volume * 0.02  # Conservative 2% estimate
                roi = 0.02  # Expected based on research
                risk_score = self._calculate_risk_score(market, 'whale')

                market_name = (market.get('question') or
                             market.get('title') or
                             market.get('description') or
                             'Unknown Market')[:80]

                market_id = (market.get('condition_id') or
                           market.get('id') or
                           market.get('market_id') or
                           'unknown')

                return ArbitrageOpportunity(
                    market_id=str(market_id),
                    market_name=market_name,
                    opportunity_type='whale',
                    expected_profit=expected_profit,
                    roi=roi,
                    capital_required=total_whale_volume * 0.1,  # 10% position
                    risk_score=risk_score,
                    urgency='high',
                    details={
                        'whale_count': len(whale_trades),
                        'total_whale_volume': f"${total_whale_volume:,.0f}",
                        'flow_imbalance': f"{flow_imbalance:.1%}",
                        'dominant_side': 'BUY' if flow_imbalance > 0 else 'SELL',
                        'recent_whale_size': f"${recent_whale['value']:,.0f}",
                        'signal_strength': 'STRONG' if abs(flow_imbalance) > 0.6 else 'MODERATE'
                    },
                    timestamp=datetime.now()
                )
        except Exception as e:
            logger.debug(f"Error in whale detection: {e}")

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

            elif strategy_type == 'whale':
                # False positive risk
                risk += 0.15

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

    def generate_summary(self, opportunities: List[ArbitrageOpportunity]):
        """Generate summary statistics"""
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
        print("📊 SCAN SUMMARY")
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

                if (i + 1) % 10 == 0:  # Progress update every 10 markets
                    logger.info(f"  [{i+1}/{len(markets)}] Processing...")

                # Get tokens
                tokens = (market.get('tokens') or
                        market.get('outcomes') or
                        market.get('options') or [])

                if not tokens:
                    continue

                # Fetch orderbooks for all tokens
                orderbooks = {}
                for token in tokens[:10]:  # Limit to 10 tokens max
                    token_id = token.get('token_id') or token.get('id')
                    if token_id:
                        book = await client.get_orderbook(token_id)
                        if book:
                            orderbooks[token_id] = book
                        await asyncio.sleep(0.05)  # Rate limiting

                # Detect Single-Condition Arbitrage
                if len(tokens) == 2:
                    token1_id = tokens[0].get('token_id') or tokens[0].get('id')
                    token2_id = tokens[1].get('token_id') or tokens[1].get('id')

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

                # Strategy 3: Whale Tracking (sample 20% of markets to save API calls)
                if i % 5 == 0:  # Check every 5th market
                    market_id = (market.get('condition_id') or
                               market.get('id') or
                               market.get('market_id'))

                    if market_id:
                        trades = await client.get_market_trades(market_id)
                        opp = self.detector.detect_whale_activity(market, trades)

                        if opp:
                            opportunities.append(opp)
                            self.alert_manager.display_opportunity(opp)

                await asyncio.sleep(0.1)  # Rate limiting

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
            batch_size = 25
            all_opportunities = []

            for i in range(0, len(all_markets), batch_size):
                batch = all_markets[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(all_markets) + batch_size - 1) // batch_size

                batch_opps = await self.analyze_market_batch(
                    batch, client, batch_num, total_batches
                )
                all_opportunities.extend(batch_opps)

                # Small delay between batches
                await asyncio.sleep(0.5)

            # Generate summary
            self.alert_manager.generate_summary(all_opportunities)

            logger.info(f"✓ Scan #{self.scan_count} complete. Found {len(all_opportunities)} opportunities.")
            logger.info(f"⏰ Next scan in {self.scan_interval} seconds...\n")

    async def run_continuous(self):
        """Run continuous monitoring - FULL MARKET"""
        logger.info("🚀 Prediction Market Arbitrage Bot Starting (FULL MARKET MODE)...")
        logger.info(f"📊 Scanning ALL available markets")
        logger.info(f"⏰ Scan interval: {self.scan_interval} seconds")
        logger.info(f"💰 Minimum profit threshold: ${self.detector.MIN_PROFIT_THRESHOLD*100:.1f} cents")
        logger.info("\nStrategies Active:")
        logger.info("  1. Single-Condition Arbitrage (YES+NO≠$1.00)")
        logger.info("  2. NegRisk Rebalancing (Σprices≠1.00, 29× efficiency)")
        logger.info("  3. Whale Tracking (>$5K trades)")
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
║    • Whale Tracking: Top performer $2.01M (4,049 trades)                 ║
║                                                                           ║
║  🔥 FULL MARKET MODE: Scanning ALL available markets                     ║
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
