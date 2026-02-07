use crate::config::TradeConfig;
use crate::models::{PairRecord, TokenSide};
use crate::time_utils::now_ts_ms;
use anyhow::{anyhow, Context, Result};
use ethers_contract::abigen;
use ethers_core::types::transaction::eip712::{EIP712Domain, Eip712DomainType, TypedData};
use ethers_core::types::{Address, Bytes, H256, U256};
use ethers_core::utils::to_checksum;
use ethers_middleware::SignerMiddleware;
use ethers_providers::{Http, Middleware, Provider};
use ethers_signers::{LocalWallet, Signer};
use rand::Rng;
use reqwest::{header::HeaderMap, Client, Method, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::warn;

fn mask_key(key: &str) -> String {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return "<empty>".to_string();
    }
    let len = trimmed.chars().count();
    if len <= 8 {
        return format!("len={}", len);
    }
    let prefix: String = trimmed.chars().take(4).collect();
    let suffix: String = trimmed.chars().rev().take(4).collect::<String>().chars().rev().collect();
    format!("{}...{}(len={})", prefix, suffix, len)
}

const OPINION_REQID_SAMPLE_RATE: u64 = 20;
const OPINION_REQID_SAMPLE_TRUNCATE: usize = 200;
static OPINION_REQID_SAMPLE: AtomicU64 = AtomicU64::new(0);

fn should_sample_request_id() -> bool {
    let count = OPINION_REQID_SAMPLE.fetch_add(1, Ordering::Relaxed);
    count % OPINION_REQID_SAMPLE_RATE == 0
}

fn truncate_text(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut out = text.chars().take(max_len).collect::<String>();
    out.push_str("...");
    out
}

fn extract_request_id(headers: &HeaderMap) -> Option<String> {
    let candidates = [
        "x-request-id",
        "x-requestid",
        "request-id",
        "x-correlation-id",
    ];
    for name in candidates {
        if let Some(value) = headers.get(name) {
            if let Ok(value) = value.to_str() {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

fn log_request_id_sample(
    method: &Method,
    url: &Url,
    status: Option<reqwest::StatusCode>,
    request_id: Option<&str>,
    response_raw: &str,
) {
    if !should_sample_request_id() {
        return;
    }
    let response_prefix = truncate_text(response_raw, OPINION_REQID_SAMPLE_TRUNCATE);
    let request_id = request_id.unwrap_or("<none>");
    let status_out = status
        .map(|code| code.as_u16().to_string())
        .unwrap_or_else(|| "<none>".to_string());
    warn!(
        "opinion_openapi request_id_sample method={} url={} status={} request_id={} response_prefix=\"{}\"",
        method,
        url,
        status_out,
        request_id,
        response_prefix
    );
}

fn is_order_endpoint(path: &str) -> bool {
    matches!(path, "/openapi/order" | "/openapi/order/cancel")
}

const DOMAIN_NAME: &str = "OPINION CTF Exchange";
const DOMAIN_VERSION: &str = "1";
const LIMIT_ORDER: i64 = 2;
const POLY_GNOSIS_SAFE_SIGNATURE_TYPE: u8 = 2;
const MAX_DECIMALS: u32 = 18;
const ORDER_ID_RECOVERY_MAX_ATTEMPTS: usize = 3;
const ORDER_ID_RECOVERY_RETRY_MS: u64 = 150;
const ORDER_ID_RECOVERY_PAGE_LIMIT: i64 = 20;
const ORDER_ID_RECOVERY_MAX_PAGES: i64 = 3;
const ORDER_ID_RECOVERY_LOOKBACK_MS: i64 = 2 * 60 * 1000;
const ENABLE_TRADING_CHECK_INTERVAL_SEC: i64 = 3600;
const DEFAULT_MULTISEND_ADDRESS_BSC: &str = "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526";
const QUOTE_TOKEN_CACHE_TTL_MS: i64 = 60 * 60 * 1000;
const MARKET_CACHE_TTL_MS: i64 = 5 * 60 * 1000;
const OPINION_UA: &str = "OpenAPI-Generator/0.2.1/python";
const DEFAULT_CONDITIONAL_TOKENS_ADDRESS: &str = "0xAD1a38cEc043e70E83a3eC30443dB285ED10D774";

abigen!(
    Erc20,
    r#"[
        function approve(address spender, uint256 amount) external returns (bool)
        function allowance(address owner, address spender) external view returns (uint256)
        function decimals() external view returns (uint8)
    ]"#
);

abigen!(
    Erc1155,
    r#"[
        function setApprovalForAll(address operator, bool approved) external
        function isApprovedForAll(address account, address operator) external view returns (bool)
    ]"#
);

abigen!(
    GnosisSafe,
    r#"[
        function getOwners() external view returns (address[])
        function getThreshold() external view returns (uint256)
        function nonce() external view returns (uint256)
        function getTransactionHash(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,uint256 _nonce) external view returns (bytes32)
        function execTransaction(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,bytes signatures) external returns (bool)
    ]"#
);

abigen!(
    MultiSendContract,
    r#"[
        function multiSend(bytes transactions) external
    ]"#
);

#[derive(Debug, Clone)]
struct TradingEnableState {
    enabled: bool,
    blocked_until_ms: i64,
    last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpinionOrderTarget {
    pub market_id: i64,
    pub token_id: String,
}

#[derive(Debug, Clone)]
pub struct PlacedOrder {
    pub order_id: String,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderAmount {
    Quote(f64),
    Base(f64),
}

#[derive(Debug, Clone)]
pub struct TradeFill {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub side: Option<String>,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub ts_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct QuoteTokenInfo {
    quote_token_address: String,
    ctf_exchange_address: String,
    decimal: u32,
    chain_id: u64,
}

#[derive(Debug, Clone)]
struct MarketInfo {
    quote_token: String,
    chain_id: u64,
    parent_market_id: Option<i64>,
    parent_title: Option<String>,
}

#[derive(Debug, Default)]
struct QuoteTokenCache {
    fetched_ms: i64,
    tokens: Vec<QuoteTokenInfo>,
}

#[derive(Debug, Clone)]
struct MarketCacheEntry {
    fetched_ms: i64,
    market: MarketInfo,
}

#[derive(Debug)]
pub struct OpinionPrivateClient {
    base_url: String,
    api_key: String,
    wallet: LocalWallet,
    signer_address: Address,
    multi_sig_address: Option<Address>,
    multisend_address: Option<Address>,
    chain_id: u64,
    rpc_url: Option<String>,
    conditional_tokens_address: Option<String>,
    client: Client,
    check_approval: bool,
    enable_retry_ms: i64,
    force_eoa_orders: bool,
    trading_state: RwLock<TradingEnableState>,
    enable_trading_last_ms: RwLock<Option<i64>>,
    token_decimals: RwLock<HashMap<Address, u8>>,
    quote_tokens: RwLock<QuoteTokenCache>,
    markets: RwLock<HashMap<i64, MarketCacheEntry>>,
}

impl OpinionPrivateClient {
    pub fn new(
        base_url: String,
        api_key: String,
        private_key: String,
        multi_sig_address: Option<String>,
        multisend_address: Option<String>,
        chain_id: u64,
        rpc_url: Option<String>,
        conditional_tokens_address: Option<String>,
        check_approval: bool,
        enable_retry_ms: i64,
        force_eoa_orders: bool,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| Client::new());
        let key = normalize_private_key(&private_key);
        let wallet: LocalWallet = key
            .parse::<LocalWallet>()
            .context("invalid opinion private_key")?
            .with_chain_id(chain_id);
        let signer_address = wallet.address();
        let multi_sig_address = match multi_sig_address {
            Some(addr) if !addr.trim().is_empty() => Some(parse_address(&addr)?),
            _ => None,
        };
        let multisend_address = match multisend_address {
            Some(addr) if !addr.trim().is_empty() => Some(parse_address(&addr)?),
            _ => None,
        };
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            wallet,
            signer_address,
            multi_sig_address,
            multisend_address,
            chain_id,
            rpc_url,
            conditional_tokens_address,
            client,
            check_approval,
            enable_retry_ms,
            force_eoa_orders,
            trading_state: RwLock::new(TradingEnableState {
                enabled: true,
                blocked_until_ms: 0,
                last_error: None,
            }),
            enable_trading_last_ms: RwLock::new(None),
            token_decimals: RwLock::new(HashMap::new()),
            quote_tokens: RwLock::new(QuoteTokenCache::default()),
            markets: RwLock::new(HashMap::new()),
        })
    }

    pub async fn place_limit_order(
        &self,
        market_id: i64,
        token_id: &str,
        side: OrderSide,
        price: f64,
        amount: OrderAmount,
        client_order_id: Option<&str>,
        _post_only: bool,
    ) -> Result<PlacedOrder> {
        self.maybe_wait_for_trading_enable().await?;
        let market = self.get_market_info(market_id).await?;
        if market.chain_id != self.chain_id {
            return Err(anyhow!(
                "opinion market chain mismatch market_chain_id={} account_chain_id={}",
                market.chain_id,
                self.chain_id
            ));
        }
        let quote_token = self.get_quote_token_info(&market.quote_token).await?;
        let exchange_address =
            parse_address(&quote_token.ctf_exchange_address).with_context(|| {
                format!(
                    "invalid ctfExchangeAddress {}",
                    quote_token.ctf_exchange_address
                )
            })?;
        let maker_address = self
            .multi_sig_address
            .ok_or_else(|| anyhow!("opinion multi_sig_address missing"))?;
        let price_str = validate_price(price)?;
        let maker_amount = compute_maker_amount(side, amount, &price_str)?;
        let maker_amount_wei = safe_amount_to_wei(maker_amount, quote_token.decimal)?;
        let (recalculated_maker_amount, taker_amount) =
            calculate_order_amounts(&price_str, maker_amount_wei, side)?;
        let token_id = U256::from_dec_str(token_id).context("invalid token_id")?;
        let order_side = side.as_int();
        let order = OrderPayload {
            salt: generate_salt(),
            maker: maker_address,
            signer: self.signer_address,
            taker: Address::zero(),
            token_id,
            maker_amount: recalculated_maker_amount,
            taker_amount,
            expiration: U256::zero(),
            nonce: U256::zero(),
            fee_rate_bps: U256::zero(),
            side: order_side as u8,
            signature_type: POLY_GNOSIS_SAFE_SIGNATURE_TYPE,
        };
        let signature = self.sign_order(&order, exchange_address).await?;
        tracing::info!(
            "opinion_order_signature market_id={} token_id={} side={} signer={} signature={}",
            market_id,
            token_id,
            order_side,
            to_checksum(&order.signer, None),
            signature
        );
        let payload = json!({
            "salt": order.salt.to_string(),
            "topicId": market_id,
            "maker": to_checksum(&order.maker, None),
            "signer": to_checksum(&order.signer, None),
            "taker": to_checksum(&order.taker, None),
            "tokenId": order.token_id.to_string(),
            "makerAmount": order.maker_amount.to_string(),
            "takerAmount": order.taker_amount.to_string(),
            "expiration": order.expiration.to_string(),
            "nonce": order.nonce.to_string(),
            "feeRateBps": order.fee_rate_bps.to_string(),
            "side": order.side.to_string(),
            "signatureType": order.signature_type.to_string(),
            "signature": signature,
            "sign": signature,
            "contractAddress": "",
            "currencyAddress": quote_token.quote_token_address,
            "price": price_str,
            "tradingMethod": LIMIT_ORDER,
            "timestamp": (now_ts_ms() / 1000) as i64,
            "safeRate": "0",
            "orderExpTime": "0"
        });
        let payload_str =
            serde_json::to_string(&payload).unwrap_or_else(|_| "<json_error>".to_string());
        tracing::info!(
            "opinion_order_payload market_id={} parent_market_id={} parent_title={} token_id={} side={} price={} price_raw={} payload={}",
            market_id,
            market.parent_market_id
                .map(|v| v.to_string())
                .unwrap_or_else(|| "<none>".to_string()),
            market.parent_title.as_deref().unwrap_or("<none>"),
            token_id,
            order_side,
            price_str,
            price,
            payload_str
        );
        let (response, response_raw) = match self
            .request_json_with_raw(Method::POST, "/openapi/order", Some(payload.clone()), None)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                if is_errno(&err, 10014) {
                    self.pause_trading(&err.to_string()).await;
                    if self.check_approval {
                        if let Ok(()) = self.enable_trading().await {
                            self.resume_trading().await;
                            match self
                                .request_json_with_raw(
                                    Method::POST,
                                    "/openapi/order",
                                    Some(payload),
                                    None,
                                )
                                .await
                            {
                                Ok(value) => value,
                                Err(err) => {
                                    if is_errno(&err, 10014) {
                                        self.pause_trading(&err.to_string()).await;
                                    }
                                    return Err(err);
                                }
                            }
                        } else {
                            return Err(err);
                        }
                    } else {
                        return Err(err);
                    }
                } else {
                    return Err(err);
                }
            }
        };
        let order_id = extract_order_id(&response)
            .unwrap_or_else(|| "".to_string());
        if order_id.is_empty() {
            let client_id = client_order_id.unwrap_or("<none>");
            if let Some(recovered_id) = self
                .recover_order_id_from_open_orders(
                    market_id,
                    side,
                    &price_str,
                    client_order_id,
                )
                .await
            {
                tracing::warn!(
                    "opinion order_id missing recovered market_id={} token_id={} side={} price={} client_order_id={} recovered_order_id={} response_raw={} response_parsed={}",
                    market_id,
                    token_id,
                    order_side,
                    price_str,
                    client_id,
                    recovered_id,
                    response_raw,
                    serde_json::to_string(&response)
                        .unwrap_or_else(|_| "<response_json_error>".to_string())
                );
                self.resume_trading().await;
                return Ok(PlacedOrder {
                    order_id: recovered_id,
                    client_order_id: client_order_id.map(|v| v.to_string()),
                });
            }
            tracing::warn!(
                "opinion order_id missing market_id={} token_id={} side={} price={} client_order_id={} response_raw={} response_parsed={}",
                market_id,
                token_id,
                order_side,
                price_str,
                client_id,
                response_raw,
                serde_json::to_string(&response)
                    .unwrap_or_else(|_| "<response_json_error>".to_string())
            );
            return Err(anyhow!("opinion order_id missing"));
        }
        self.resume_trading().await;
        Ok(PlacedOrder {
            order_id,
            client_order_id: client_order_id.map(|v| v.to_string()),
        })
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let order_id = order_id.trim();
        if order_id.is_empty() {
            warn!("opinion_cancel order_id empty");
            return Err(anyhow!("opinion cancel order_id empty"));
        }
        match self
            .request_json(
                Method::POST,
                "/openapi/order/cancel",
                Some(json!({ "orderId": order_id })),
                None,
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                if !is_errno(&err, 10207) {
                    return Err(err);
                }
                tracing::info!(
                    "opinion_cancel order not found treated as ok order_id={} err={}",
                    order_id,
                    err
                );
            }
        }
        Ok(())
    }

    pub async fn fetch_open_order_ids(
        &self,
        market_id: i64,
        max_pages: i64,
        limit: i64,
    ) -> Result<(HashSet<String>, bool)> {
        let mut order_ids = HashSet::new();
        let mut complete = true;
        if max_pages <= 0 || limit <= 0 {
            return Ok((order_ids, true));
        }
        for page in 1..=max_pages {
            let list = self.fetch_open_orders_page(market_id, page, limit).await?;
            if list.is_empty() {
                break;
            }
            for entry in list.iter() {
                if let Some(order_id) = extract_order_id(entry) {
                    order_ids.insert(order_id);
                }
            }
            if list.len() < limit as usize {
                break;
            }
            if page == max_pages {
                complete = false;
            }
        }
        Ok((order_ids, complete))
    }

    async fn fetch_open_orders_page(
        &self,
        market_id: i64,
        page: i64,
        limit: i64,
    ) -> Result<Vec<Value>> {
        let mut params = vec![
            ("page", page.to_string()),
            ("limit", limit.to_string()),
            ("chainId", self.chain_id.to_string()),
            ("status", "1".to_string()),
        ];
        if market_id > 0 {
            params.push(("marketId", market_id.to_string()));
        }
        let response = self
            .request_json(Method::GET, "/openapi/order", None, Some(&params))
            .await?;
        let list = response
            .get("list")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(list)
    }

    async fn recover_order_id_from_open_orders(
        &self,
        market_id: i64,
        side: OrderSide,
        price_str: &str,
        client_order_id: Option<&str>,
    ) -> Option<String> {
        let client_order_id = client_order_id?.trim();
        if client_order_id.is_empty() {
            return None;
        }
        let expected_side = side.as_int() as i64;
        let expected_price = price_str.parse::<f64>().ok();
        let now_ms = now_ts_ms();
        let mut last_err: Option<String> = None;
        let mut ambiguous_count: Option<usize> = None;

        for attempt in 0..ORDER_ID_RECOVERY_MAX_ATTEMPTS {
            if attempt > 0 {
                let delay_ms = ORDER_ID_RECOVERY_RETRY_MS.saturating_mul((attempt + 1) as u64);
                sleep(std::time::Duration::from_millis(delay_ms)).await;
            }

            let mut attr_candidates: Vec<(String, i64)> = Vec::new();
            for page in 1..=ORDER_ID_RECOVERY_MAX_PAGES {
                let list = match self
                    .fetch_open_orders_page(market_id, page, ORDER_ID_RECOVERY_PAGE_LIMIT)
                    .await
                {
                    Ok(list) => list,
                    Err(err) => {
                        last_err = Some(err.to_string());
                        break;
                    }
                };
                if list.is_empty() {
                    break;
                }
                for entry in list.iter() {
                    if let Some(entry_client_id) = extract_client_order_id(entry) {
                        if entry_client_id == client_order_id {
                            if let Some(order_id) = extract_order_id(entry) {
                                return Some(order_id);
                            }
                        }
                        continue;
                    }
                    let created_ms = match extract_created_ms(entry) {
                        Some(ts) if now_ms.saturating_sub(ts) <= ORDER_ID_RECOVERY_LOOKBACK_MS => ts,
                        _ => continue,
                    };
                    if !order_matches_attributes(entry, market_id, expected_side, expected_price) {
                        continue;
                    }
                    if let Some(order_id) = extract_order_id(entry) {
                        attr_candidates.push((order_id, created_ms));
                    }
                }
                if list.len() < ORDER_ID_RECOVERY_PAGE_LIMIT as usize {
                    break;
                }
            }

            if attr_candidates.len() == 1 {
                return Some(attr_candidates[0].0.clone());
            }
            if attr_candidates.len() > 1 {
                ambiguous_count = Some(attr_candidates.len());
                continue;
            }
        }

        if let Some(count) = ambiguous_count {
            warn!(
                "opinion order_id recovery ambiguous market_id={} client_order_id={} candidates={}",
                market_id,
                client_order_id,
                count
            );
        }
        if let Some(err) = last_err {
            warn!(
                "opinion order_id recovery failed market_id={} client_order_id={} err={}",
                market_id,
                client_order_id,
                err
            );
        }
        None
    }

    pub async fn fetch_trades(
        &self,
        market_id: i64,
        _token_id: &str,
        since_ms: Option<i64>,
    ) -> Result<Vec<TradeFill>> {
        let mut params = vec![
            ("page", "1".to_string()),
            ("limit", "50".to_string()),
            ("chainId", self.chain_id.to_string()),
        ];
        params.push(("marketId", market_id.to_string()));
        let response = self
            .request_json(Method::GET, "/openapi/trade", None, Some(&params))
            .await
            .or_else(|err| {
                if is_errno(&err, 10207) {
                    Ok(json!({ "list": [] }))
                } else {
                    Err(err)
                }
            })?;
        let list = response
            .get("list")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_else(Vec::new);
        let mut fills = Vec::new();
        for entry in list {
            let ts_ms = extract_ts_ms(&entry);
            if let (Some(since), Some(ts)) = (since_ms, ts_ms) {
                if ts < since {
                    continue;
                }
            }
            fills.push(TradeFill {
                order_id: extract_string(&entry, &["orderId", "order_id", "id"]),
                client_order_id: extract_string(&entry, &["clientOrderId", "client_order_id"]),
                side: extract_string(&entry, &["side", "orderSide"]),
                price: extract_f64_from_value(&entry, &["price", "orderPrice", "matchedPrice"]),
                size: extract_f64_from_value(
                    &entry,
                    &["size", "makerAmount", "filledSize", "matchedAmount", "fillSize"],
                ),
                ts_ms,
            });
        }
        Ok(fills)
    }

    pub async fn enable_trading(&self) -> Result<()> {
        // Use on-chain approval state machine only. REST enableTrading path is optional and
        // can change; on-chain approvals are authoritative for trading readiness.
        let now = now_ts_ms();
        {
            let last = self.enable_trading_last_ms.read().await;
            if let Some(last_ms) = *last {
                if now - last_ms < ENABLE_TRADING_CHECK_INTERVAL_SEC * 1000 {
                    tracing::info!(
                        "opinion_enable_trading cached last={} interval_sec={}",
                        last_ms,
                        ENABLE_TRADING_CHECK_INTERVAL_SEC
                    );
                    return Ok(());
                }
            }
        }
        {
            let mut last = self.enable_trading_last_ms.write().await;
            *last = Some(now);
        }
        tracing::info!(
            "opinion_enable_trading start signer={} safe={}",
            to_checksum(&self.signer_address, None),
            self.multi_sig_address
                .map(|addr| to_checksum(&addr, None))
                .unwrap_or_else(|| "<none>".to_string())
        );
        match self.enable_trading_onchain().await {
            Ok(()) => {
                tracing::info!("opinion_enable_trading success");
                Ok(())
            }
            Err(err) => {
                tracing::warn!("opinion_enable_trading failed err={}", err);
                Err(err)
            }
        }
    }

    async fn sign_order(&self, order: &OrderPayload, verifying_contract: Address) -> Result<String> {
        let mut types: BTreeMap<String, Vec<Eip712DomainType>> = BTreeMap::new();
        types.insert(
            "EIP712Domain".to_string(),
            vec![
                Eip712DomainType {
                    name: "name".to_string(),
                    r#type: "string".to_string(),
                },
                Eip712DomainType {
                    name: "version".to_string(),
                    r#type: "string".to_string(),
                },
                Eip712DomainType {
                    name: "chainId".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "verifyingContract".to_string(),
                    r#type: "address".to_string(),
                },
            ],
        );
        types.insert(
            "Order".to_string(),
            vec![
                Eip712DomainType {
                    name: "salt".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "maker".to_string(),
                    r#type: "address".to_string(),
                },
                Eip712DomainType {
                    name: "signer".to_string(),
                    r#type: "address".to_string(),
                },
                Eip712DomainType {
                    name: "taker".to_string(),
                    r#type: "address".to_string(),
                },
                Eip712DomainType {
                    name: "tokenId".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "makerAmount".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "takerAmount".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "expiration".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "nonce".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "feeRateBps".to_string(),
                    r#type: "uint256".to_string(),
                },
                Eip712DomainType {
                    name: "side".to_string(),
                    r#type: "uint8".to_string(),
                },
                Eip712DomainType {
                    name: "signatureType".to_string(),
                    r#type: "uint8".to_string(),
                },
            ],
        );
        let mut message = BTreeMap::new();
        message.insert("salt".to_string(), json!(order.salt.to_string()));
        message.insert("maker".to_string(), json!(to_checksum(&order.maker, None)));
        message.insert("signer".to_string(), json!(to_checksum(&order.signer, None)));
        message.insert("taker".to_string(), json!(to_checksum(&order.taker, None)));
        message.insert("tokenId".to_string(), json!(order.token_id.to_string()));
        message.insert(
            "makerAmount".to_string(),
            json!(order.maker_amount.to_string()),
        );
        message.insert(
            "takerAmount".to_string(),
            json!(order.taker_amount.to_string()),
        );
        message.insert(
            "expiration".to_string(),
            json!(order.expiration.to_string()),
        );
        message.insert("nonce".to_string(), json!(order.nonce.to_string()));
        message.insert(
            "feeRateBps".to_string(),
            json!(order.fee_rate_bps.to_string()),
        );
        message.insert("side".to_string(), json!(order.side.to_string()));
        message.insert(
            "signatureType".to_string(),
            json!(order.signature_type.to_string()),
        );
        let typed_data = TypedData {
            domain: EIP712Domain {
                name: Some(DOMAIN_NAME.to_string()),
                version: Some(DOMAIN_VERSION.to_string()),
                chain_id: Some(U256::from(self.chain_id)),
                verifying_contract: Some(verifying_contract),
                salt: None,
            },
            types,
            primary_type: "Order".to_string(),
            message,
        };
        let signature = self
            .wallet
            .sign_typed_data(&typed_data)
            .await
            .context("opinion sign_typed_data failed")?;
        Ok(ensure_0x(signature.to_string()))
    }

    async fn enable_trading_onchain(&self) -> Result<()> {
        if !self.force_eoa_orders && self.multi_sig_address.is_some() {
            return self.enable_trading_onchain_safe().await;
        }
        self.enable_trading_onchain_eoa().await
    }

    async fn enable_trading_onchain_eoa(&self) -> Result<()> {
        let rpc_url = self
            .rpc_url
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("opinion rpc_url missing in account config"))?;
        let provider = Provider::<Http>::try_from(rpc_url)
            .context("opinion rpc_url parse failed")?;
        let client = Arc::new(SignerMiddleware::new(provider, self.wallet.clone()));
        self.check_gas_balance(&client, self.signer_address, 500_000)
            .await?;
        let my_addr = client.address();
        let tokens = self.get_quote_tokens().await?;
        let mut exchange_addresses: HashSet<Address> = HashSet::new();
        let mut matched = 0usize;
        let conditional_tokens_address = self.conditional_tokens_address()?;
        for token in tokens {
            if token.chain_id != self.chain_id {
                continue;
            }
            matched += 1;
            let quote_token_address =
                parse_address(&token.quote_token_address).with_context(|| {
                    format!("invalid quoteTokenAddress {}", token.quote_token_address)
                })?;
            let exchange_address =
                parse_address(&token.ctf_exchange_address).with_context(|| {
                    format!("invalid ctfExchangeAddress {}", token.ctf_exchange_address)
                })?;
            exchange_addresses.insert(exchange_address);
            let erc20 = Erc20::new(quote_token_address, client.clone());
            let decimals = self.get_token_decimals(&erc20, quote_token_address).await?;
            let min_threshold = min_threshold(decimals);
            let allowance = erc20
                .allowance(my_addr, exchange_address)
                .call()
                .await
                .context("opinion erc20 allowance call failed")?;
            if allowance < min_threshold {
                if allowance > U256::zero() {
                    let call = erc20.approve(exchange_address, U256::zero());
                    let pending = call
                        .send()
                        .await
                        .context("opinion erc20 approve reset failed")?;
                    let tx_hash = pending.tx_hash();
                    let receipt = pending
                        .await
                        .context("opinion erc20 approve reset receipt failed")?;
                    if let Some(receipt) = receipt {
                        tracing::info!(
                            "opinion_enable_trading erc20 reset receipt tx={:#x} status={:?}",
                            tx_hash,
                            receipt.status
                        );
                    }
                    tracing::info!(
                        "opinion_enable_trading erc20 reset token={} exchange={} tx={:#x}",
                        quote_token_address,
                        exchange_address,
                        tx_hash
                    );
                }
                let call = erc20.approve(exchange_address, U256::MAX);
                let pending = call
                    .send()
                    .await
                    .context("opinion erc20 approve failed")?;
                let tx_hash = pending.tx_hash();
                let receipt = pending
                    .await
                    .context("opinion erc20 approve receipt failed")?;
                if let Some(receipt) = receipt {
                    tracing::info!(
                        "opinion_enable_trading erc20 approve receipt tx={:#x} status={:?}",
                        tx_hash,
                        receipt.status
                    );
                }
                tracing::info!(
                    "opinion_enable_trading erc20 approved token={} exchange={} tx={:#x}",
                    quote_token_address,
                    exchange_address,
                    tx_hash
                );
            } else {
                tracing::info!(
                    "opinion_enable_trading erc20 allowance ok token={} exchange={} allowance={}",
                    quote_token_address,
                    exchange_address,
                    allowance
                );
            }
            let allowance = erc20
                .allowance(my_addr, conditional_tokens_address)
                .call()
                .await
                .context("opinion erc20 conditionalTokens allowance call failed")?;
            if allowance < min_threshold {
                if allowance > U256::zero() {
                    let call = erc20.approve(conditional_tokens_address, U256::zero());
                    let pending = call
                        .send()
                        .await
                        .context("opinion erc20 conditionalTokens approve reset failed")?;
                    let tx_hash = pending.tx_hash();
                    let receipt = pending
                        .await
                        .context("opinion erc20 conditionalTokens approve reset receipt failed")?;
                    if let Some(receipt) = receipt {
                        tracing::info!(
                            "opinion_enable_trading erc20 conditionalTokens reset receipt tx={:#x} status={:?}",
                            tx_hash,
                            receipt.status
                        );
                    }
                    tracing::info!(
                        "opinion_enable_trading erc20 reset token={} conditional_tokens={} tx={:#x}",
                        quote_token_address,
                        conditional_tokens_address,
                        tx_hash
                    );
                }
                let call = erc20.approve(conditional_tokens_address, U256::MAX);
                let pending = call
                    .send()
                    .await
                    .context("opinion erc20 conditionalTokens approve failed")?;
                let tx_hash = pending.tx_hash();
                let receipt = pending
                    .await
                    .context("opinion erc20 conditionalTokens approve receipt failed")?;
                if let Some(receipt) = receipt {
                    tracing::info!(
                        "opinion_enable_trading erc20 conditionalTokens approve receipt tx={:#x} status={:?}",
                        tx_hash,
                        receipt.status
                    );
                }
                tracing::info!(
                    "opinion_enable_trading erc20 approved token={} conditional_tokens={} tx={:#x}",
                    quote_token_address,
                    conditional_tokens_address,
                    tx_hash
                );
            }
        }
        if matched == 0 {
            return Err(anyhow!(
                "opinion quoteToken list empty for chain_id={}",
                self.chain_id
            ));
        }
        let erc1155 = Erc1155::new(conditional_tokens_address, client.clone());
        for exchange_address in exchange_addresses {
            let approved = erc1155
                .is_approved_for_all(my_addr, exchange_address)
                .call()
                .await
                .context("opinion erc1155 isApprovedForAll call failed")?;
            if !approved {
                let call = erc1155.set_approval_for_all(exchange_address, true);
                let pending = call
                    .send()
                    .await
                    .context("opinion erc1155 setApprovalForAll failed")?;
                let tx_hash = pending.tx_hash();
                let receipt = pending
                    .await
                    .context("opinion erc1155 approve receipt failed")?;
                if let Some(receipt) = receipt {
                    tracing::info!(
                        "opinion_enable_trading erc1155 approve receipt tx={:#x} status={:?}",
                        tx_hash,
                        receipt.status
                    );
                }
                tracing::info!(
                    "opinion_enable_trading erc1155 approved exchange={} tx={:#x}",
                    exchange_address,
                    tx_hash
                );
            } else {
                tracing::info!(
                    "opinion_enable_trading erc1155 already approved exchange={}",
                    exchange_address
                );
            }
        }
        Ok(())
    }

    async fn enable_trading_onchain_safe(&self) -> Result<()> {
        let safe_address = self
            .multi_sig_address
            .ok_or_else(|| anyhow!("opinion safe address missing"))?;
        tracing::info!(
            "opinion_enable_trading safe path entered safe={} signer={}",
            to_checksum(&safe_address, None),
            to_checksum(&self.signer_address, None)
        );
        let rpc_url = self
            .rpc_url
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("opinion rpc_url missing in account config"))?;
        let provider = Provider::<Http>::try_from(rpc_url)
            .context("opinion rpc_url parse failed")?;
        let client = Arc::new(SignerMiddleware::new(provider, self.wallet.clone()));
        self.check_gas_balance(&client, self.signer_address, 500_000)
            .await?;
        let safe = GnosisSafe::new(safe_address, client.clone());
        let multisend_address = self.multisend_address()?;
        let multisend = MultiSendContract::new(multisend_address, client.clone());
        let tokens = self.get_quote_tokens().await?;
        let mut exchange_addresses: HashSet<Address> = HashSet::new();
        let mut matched = 0usize;
        let mut multisend_txs: Vec<MultiSendTx> = Vec::new();
        let conditional_tokens_address = self.conditional_tokens_address()?;
        for token in tokens {
            if token.chain_id != self.chain_id {
                continue;
            }
            matched += 1;
            let quote_token_address =
                parse_address(&token.quote_token_address).with_context(|| {
                    format!("invalid quoteTokenAddress {}", token.quote_token_address)
                })?;
            let exchange_address =
                parse_address(&token.ctf_exchange_address).with_context(|| {
                    format!("invalid ctfExchangeAddress {}", token.ctf_exchange_address)
                })?;
            exchange_addresses.insert(exchange_address);
            let erc20 = Erc20::new(quote_token_address, client.clone());
            let decimals = self.get_token_decimals(&erc20, quote_token_address).await?;
            let min_threshold = min_threshold(decimals);
            let allowance = erc20
                .allowance(safe_address, exchange_address)
                .call()
                .await
                .context("opinion erc20 allowance call failed")?;
            if allowance < min_threshold {
                if allowance > U256::zero() {
                    let data = erc20
                        .approve(exchange_address, U256::zero())
                        .calldata()
                        .context("opinion erc20 approve calldata failed")?;
                    multisend_txs.push(MultiSendTx {
                        operation: 0,
                        to: quote_token_address,
                        value: U256::zero(),
                        data,
                    });
                }
                let data = erc20
                    .approve(exchange_address, U256::MAX)
                    .calldata()
                    .context("opinion erc20 approve calldata failed")?;
                multisend_txs.push(MultiSendTx {
                    operation: 0,
                    to: quote_token_address,
                    value: U256::zero(),
                    data,
                });
            } else {
                tracing::info!(
                    "opinion_enable_trading safe erc20 allowance ok token={} exchange={} allowance={}",
                    quote_token_address,
                    exchange_address,
                    allowance
                );
            }
            let allowance = erc20
                .allowance(safe_address, conditional_tokens_address)
                .call()
                .await
                .context("opinion erc20 conditionalTokens allowance call failed")?;
            if allowance < min_threshold {
                if allowance > U256::zero() {
                    let data = erc20
                        .approve(conditional_tokens_address, U256::zero())
                        .calldata()
                        .context("opinion erc20 conditionalTokens calldata failed")?;
                    multisend_txs.push(MultiSendTx {
                        operation: 0,
                        to: quote_token_address,
                        value: U256::zero(),
                        data,
                    });
                }
                let data = erc20
                    .approve(conditional_tokens_address, U256::MAX)
                    .calldata()
                    .context("opinion erc20 conditionalTokens calldata failed")?;
                multisend_txs.push(MultiSendTx {
                    operation: 0,
                    to: quote_token_address,
                    value: U256::zero(),
                    data,
                });
            }
        }
        if matched == 0 {
            return Err(anyhow!(
                "opinion quoteToken list empty for chain_id={}",
                self.chain_id
            ));
        }
        tracing::info!(
            "opinion_enable_trading erc1155 check safe={} conditional_tokens={}",
            to_checksum(&safe_address, None),
            to_checksum(&conditional_tokens_address, None)
        );
        let erc1155 = Erc1155::new(conditional_tokens_address, client.clone());
        for exchange_address in exchange_addresses {
            let approved = match erc1155
                .is_approved_for_all(safe_address, exchange_address)
                .call()
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        "opinion_enable_trading erc1155 isApprovedForAll failed safe={} exchange={} conditional_tokens={} err={}",
                        to_checksum(&safe_address, None),
                        to_checksum(&exchange_address, None),
                        to_checksum(&conditional_tokens_address, None),
                        err
                    );
                    return Err(anyhow!(err).context("opinion erc1155 isApprovedForAll call failed"));
                }
            };
            if !approved {
                let data = erc1155
                    .set_approval_for_all(exchange_address, true)
                    .calldata()
                    .context("opinion erc1155 calldata failed")?;
                tracing::info!(
                    "opinion_enable_trading erc1155 setApprovalForAll safe={} exchange={} conditional_tokens={}",
                    to_checksum(&safe_address, None),
                    to_checksum(&exchange_address, None),
                    to_checksum(&conditional_tokens_address, None)
                );
                multisend_txs.push(MultiSendTx {
                    operation: 0,
                    to: conditional_tokens_address,
                    value: U256::zero(),
                    data,
                });
            } else {
                tracing::info!(
                    "opinion_enable_trading safe erc1155 already approved exchange={}",
                    exchange_address
                );
            }
        }
        if multisend_txs.is_empty() {
            tracing::info!(
                "opinion_enable_trading safe no tx executed (already approved) safe={}",
                to_checksum(&safe_address, None)
            );
            return Ok(());
        }
        let transactions = encode_multisend_txs(&multisend_txs)?;
        let data = multisend
            .multi_send(transactions)
            .calldata()
            .context("opinion multisend calldata failed")?;
        self.safe_exec_transaction(&safe, multisend_address, data, 1)
            .await
            .context("opinion safe multisend exec failed")?;
        Ok(())
    }


    async fn safe_exec_transaction(
        &self,
        safe: &GnosisSafe<SignerMiddleware<Provider<Http>, LocalWallet>>,
        to: Address,
        data: Bytes,
        operation: u8,
    ) -> Result<()> {
        let zero = Address::zero();
        let value = U256::zero();
        let safe_tx_gas = U256::zero();
        let base_gas = U256::zero();
        let gas_price = U256::zero();
        let nonce = safe
            .nonce()
            .call()
            .await
            .context("opinion safe nonce fetch failed")?;
        let safe_hash = safe
            .get_transaction_hash(
                to,
                value,
                data.clone(),
                operation,
                safe_tx_gas,
                base_gas,
                gas_price,
                zero,
                zero,
                nonce,
            )
            .call()
            .await
            .context("opinion safe getTransactionHash failed")?;
        let signature = self.wallet.sign_hash(H256::from(safe_hash))?;
        let sig_bytes = signature.to_vec();
        tracing::info!(
            "opinion_safe signature ok safe={} signer={} hash={:#x} sig_len={}",
            to_checksum(&safe.address(), None),
            to_checksum(&self.signer_address, None),
            H256::from(safe_hash),
            sig_bytes.len()
        );
        let call = safe.exec_transaction(
            to,
            value,
            data,
            operation,
            safe_tx_gas,
            base_gas,
            gas_price,
            zero,
            zero,
            Bytes::from(sig_bytes),
        );
        let pending = match call.send().await {
            Ok(pending) => pending,
            Err(err) => {
                tracing::warn!(
                    "opinion_safe execTransaction send failed safe={} signer={} to={} nonce={} err={}",
                    to_checksum(&safe.address(), None),
                    to_checksum(&self.signer_address, None),
                    to_checksum(&to, None),
                    nonce,
                    err
                );
                return Err(anyhow!(err).context("opinion safe execTransaction send failed"));
            }
        };
        let tx_hash = pending.tx_hash();
        tracing::info!(
            "opinion_safe execTransaction sent safe={} to={} tx={:#x}",
            to_checksum(&safe.address(), None),
            to_checksum(&to, None),
            tx_hash
        );
        let receipt = pending
            .await
            .context("opinion safe execTransaction receipt failed")?;
        if let Some(receipt) = receipt {
            tracing::info!(
                "opinion_safe execTransaction receipt tx={:#x} status={:?}",
                tx_hash,
                receipt.status
            );
            if receipt.status != Some(1u64.into()) {
                return Err(anyhow!(
                    "opinion safe execTransaction reverted tx={:#x}",
                    tx_hash
                ));
            }
        }
        tracing::info!("opinion_safe execTransaction ok tx={:#x}", tx_hash);
        Ok(())
    }

    fn conditional_tokens_address(&self) -> Result<Address> {
        if let Some(addr) = self
            .conditional_tokens_address
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            return parse_address(addr);
        }
        parse_address(DEFAULT_CONDITIONAL_TOKENS_ADDRESS)
    }

    fn multisend_address(&self) -> Result<Address> {
        if let Some(addr) = self.multisend_address {
            return Ok(addr);
        }
        if self.chain_id == 56 {
            return parse_address(DEFAULT_MULTISEND_ADDRESS_BSC);
        }
        Err(anyhow!(
            "opinion multisend_address missing for chain_id={}",
            self.chain_id
        ))
    }

    async fn get_token_decimals(
        &self,
        erc20: &Erc20<SignerMiddleware<Provider<Http>, LocalWallet>>,
        token: Address,
    ) -> Result<u8> {
        {
            let cache = self.token_decimals.read().await;
            if let Some(value) = cache.get(&token) {
                return Ok(*value);
            }
        }
        let value = erc20
            .decimals()
            .call()
            .await
            .unwrap_or(MAX_DECIMALS as u8);
        let mut cache = self.token_decimals.write().await;
        cache.insert(token, value);
        Ok(value)
    }

    async fn check_gas_balance(
        &self,
        client: &SignerMiddleware<Provider<Http>, LocalWallet>,
        address: Address,
        estimated_gas: u64,
    ) -> Result<()> {
        let balance = client
            .get_balance(address, None)
            .await
            .context("opinion gas balance fetch failed")?;
        let block = client
            .get_block(ethers_core::types::BlockNumber::Latest)
            .await
            .context("opinion gas block fetch failed")?;
        let base_fee = block
            .and_then(|b| b.base_fee_per_gas)
            .unwrap_or(U256::zero());
        let gas_price = if base_fee > U256::zero() {
            base_fee * U256::from(2u64) + U256::from(2_000_000_000u64)
        } else {
            client
                .get_gas_price()
                .await
                .context("opinion gas price fetch failed")?
        };
        let estimated_with_margin = (estimated_gas as u128 * 12 / 10) as u64;
        let required = gas_price * U256::from(estimated_with_margin);
        if balance < required {
            return Err(anyhow!(
                "opinion insufficient gas balance addr={} balance={} required={}",
                to_checksum(&address, None),
                balance,
                required
            ));
        }
        Ok(())
    }

    async fn maybe_wait_for_trading_enable(&self) -> Result<()> {
        let now = now_ts_ms();
        let state = self.trading_state.read().await;
        if state.enabled {
            return Ok(());
        }
        if state.blocked_until_ms > now {
            let reason = state
                .last_error
                .clone()
                .unwrap_or_else(|| "enable_required".to_string());
            return Err(anyhow!(
                "opinion trading paused until {} reason={}",
                state.blocked_until_ms,
                reason
            ));
        }
        drop(state);
        if !self.check_approval {
            return Err(anyhow!("opinion trading paused (enable required)"));
        }
        match self.enable_trading().await {
            Ok(()) => {
                self.resume_trading().await;
                Ok(())
            }
            Err(err) => {
                self.pause_trading(&err.to_string()).await;
                Err(err)
            }
        }
    }

    async fn pause_trading(&self, reason: &str) {
        let cooldown = self.enable_retry_ms.max(0);
        let mut state = self.trading_state.write().await;
        state.enabled = false;
        state.blocked_until_ms = now_ts_ms() + cooldown;
        state.last_error = Some(reason.to_string());
        tracing::warn!(
            "opinion_trading_paused until={} reason={}",
            state.blocked_until_ms,
            reason
        );
    }

    async fn resume_trading(&self) {
        let mut state = self.trading_state.write().await;
        state.enabled = true;
        state.blocked_until_ms = 0;
        state.last_error = None;
        tracing::info!("opinion_trading_resumed");
    }

    async fn get_market_info(&self, market_id: i64) -> Result<MarketInfo> {
        let now = now_ts_ms();
        {
            let cache = self.markets.read().await;
            if let Some(entry) = cache.get(&market_id) {
                if now - entry.fetched_ms <= MARKET_CACHE_TTL_MS {
                    return Ok(entry.market.clone());
                }
            }
        }
        let path = format!("/openapi/market/{market_id}");
        let value = self.request_json(Method::GET, &path, None, None).await?;
        let data = value
            .get("data")
            .cloned()
            .unwrap_or(value.clone());
        let quote_token = extract_string(&data, &["quoteToken", "quote_token"]).ok_or_else(|| {
            anyhow!("opinion market missing quoteToken market_id={}", market_id)
        })?;
        let chain_id = extract_string(&data, &["chainId", "chain_id"])
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(self.chain_id);
        let collection = data.get("collection");
        let parent_title = collection
            .and_then(|v| v.get("title"))
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());
        let parent_market_id = collection
            .and_then(|v| v.get("current"))
            .and_then(|v| extract_string(v, &["marketId", "market_id"]))
            .and_then(|v| v.parse::<i64>().ok())
            .or_else(|| {
                collection
                    .and_then(|v| v.get("next"))
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|v| extract_string(v, &["marketId", "market_id"]))
                    .and_then(|v| v.parse::<i64>().ok())
            });
        let market = MarketInfo {
            quote_token,
            chain_id,
            parent_market_id,
            parent_title,
        };
        let mut cache = self.markets.write().await;
        cache.insert(
            market_id,
            MarketCacheEntry {
                fetched_ms: now,
                market: market.clone(),
            },
        );
        Ok(market)
    }

    pub async fn prefetch_markets(
        &self,
        base_url: &str,
        market_ids: &[i64],
        interval_ms: u64,
    ) -> Result<(usize, usize)> {
        let base = base_url.trim();
        if base.is_empty() {
            return Err(anyhow!("opinion prefetch base_url empty"));
        }
        let mut unique: Vec<i64> = market_ids.iter().copied().collect();
        unique.sort_unstable();
        unique.dedup();
        if unique.is_empty() {
            return Ok((0, 0));
        }
        let now = now_ts_ms();
        let mut ok = 0usize;
        let mut failed = 0usize;
        if let Err(err) = self.prefetch_quote_tokens(base).await {
            warn!("opinion_openapi prefetch quoteToken failed: {}", err);
        }
        for market_id in unique {
            let cached = {
                let cache = self.markets.read().await;
                cache
                    .get(&market_id)
                    .map(|entry| now - entry.fetched_ms <= MARKET_CACHE_TTL_MS)
                    .unwrap_or(false)
            };
            if cached {
                ok += 1;
                continue;
            }
        let path = format!("/openapi/market/{market_id}");
            match self
                .request_json_with_base(base, Method::GET, &path, None, None)
                .await
            {
                Ok(value) => {
                    let data = value.get("data").cloned().unwrap_or(value.clone());
                    let quote_token =
                        match extract_string(&data, &["quoteToken", "quote_token"]) {
                            Some(value) => value,
                            None => {
                                failed += 1;
                                warn!(
                                    "opinion_openapi prefetch market missing quoteToken market_id={}",
                                    market_id
                                );
                                continue;
                            }
                        };
                    let chain_id = extract_string(&data, &["chainId", "chain_id"])
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(self.chain_id);
                    let collection = data.get("collection");
                    let parent_title = collection
                        .and_then(|v| v.get("title"))
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string());
                    let parent_market_id = collection
                        .and_then(|v| v.get("current"))
                        .and_then(|v| extract_string(v, &["marketId", "market_id"]))
                        .and_then(|v| v.parse::<i64>().ok())
                        .or_else(|| {
                            collection
                                .and_then(|v| v.get("next"))
                                .and_then(|v| v.as_array())
                                .and_then(|arr| arr.first())
                                .and_then(|v| extract_string(v, &["marketId", "market_id"]))
                                .and_then(|v| v.parse::<i64>().ok())
                        });
                    let market = MarketInfo {
                        quote_token,
                        chain_id,
                        parent_market_id,
                        parent_title,
                    };
                    let mut cache = self.markets.write().await;
                    cache.insert(
                        market_id,
                        MarketCacheEntry {
                            fetched_ms: now_ts_ms(),
                            market: market.clone(),
                        },
                    );
                    ok += 1;
                }
                Err(err) => {
                    failed += 1;
                    warn!(
                        "opinion_openapi prefetch market failed base={} market_id={} err={}",
                        base, market_id, err
                    );
                }
            }
            if interval_ms > 0 {
                sleep(std::time::Duration::from_millis(interval_ms)).await;
            }
        }
        Ok((ok, failed))
    }

    async fn get_quote_tokens(&self) -> Result<Vec<QuoteTokenInfo>> {
        let now = now_ts_ms();
        {
            let cache = self.quote_tokens.read().await;
            if now - cache.fetched_ms <= QUOTE_TOKEN_CACHE_TTL_MS && !cache.tokens.is_empty() {
                return Ok(cache.tokens.clone());
            }
        }
        let value = self
            .request_json(Method::GET, "/openapi/quoteToken", None, None)
            .await?;
        let list = value
            .get("list")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut tokens = Vec::new();
        for entry in list {
            let quote_token_address =
                match extract_string(&entry, &["quoteTokenAddress", "quote_token_address"]) {
                    Some(value) => value,
                    None => continue,
                };
            let ctf_exchange_address =
                match extract_string(&entry, &["ctfExchangeAddress", "ctf_exchange_address"]) {
                    Some(value) => value,
                    None => continue,
                };
            let decimal = extract_string(&entry, &["decimal"])
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(0);
            let chain_id = extract_string(&entry, &["chainId", "chain_id"])
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(self.chain_id);
            tokens.push(QuoteTokenInfo {
                quote_token_address,
                ctf_exchange_address,
                decimal,
                chain_id,
            });
        }
        if tokens.is_empty() {
            return Err(anyhow!("opinion quoteToken list empty"));
        }
        let mut cache = self.quote_tokens.write().await;
        cache.fetched_ms = now;
        cache.tokens = tokens.clone();
        Ok(tokens)
    }

    async fn get_quote_token_info(&self, quote_token_address: &str) -> Result<QuoteTokenInfo> {
        let tokens = self.get_quote_tokens().await?;
        let found = tokens
            .into_iter()
            .find(|t| t.quote_token_address.eq_ignore_ascii_case(quote_token_address))
            .ok_or_else(|| anyhow!("quote token not found for {}", quote_token_address))?;
        Ok(found)
    }

    async fn request_json(
        &self,
        method: Method,
        path: &str,
        payload: Option<Value>,
        params: Option<&[(&str, String)]>,
    ) -> Result<Value> {
        self.request_json_with_base(&self.base_url, method, path, payload, params)
            .await
    }

    async fn request_json_with_raw(
        &self,
        method: Method,
        path: &str,
        payload: Option<Value>,
        params: Option<&[(&str, String)]>,
    ) -> Result<(Value, String)> {
        self.request_json_with_base_raw(&self.base_url, method, path, payload, params)
            .await
    }

    async fn prefetch_quote_tokens(&self, base_url: &str) -> Result<()> {
        let value = self
            .request_json_with_base(base_url, Method::GET, "/openapi/quoteToken", None, None)
            .await?;
        let list = value
            .get("list")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut tokens = Vec::new();
        for entry in list {
            let quote_token_address = match extract_string(&entry, &["quoteTokenAddress", "quote_token_address"]) {
                Some(value) => value,
                None => continue,
            };
            let ctf_exchange_address = match extract_string(&entry, &["ctfExchangeAddress", "ctf_exchange_address"]) {
                Some(value) => value,
                None => continue,
            };
            let decimal = extract_string(&entry, &["decimal"])
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(0);
            let chain_id = extract_string(&entry, &["chainId", "chain_id"])
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(self.chain_id);
            tokens.push(QuoteTokenInfo {
                quote_token_address,
                ctf_exchange_address,
                decimal,
                chain_id,
            });
        }
        if tokens.is_empty() {
            return Err(anyhow!("opinion quoteToken list empty"));
        }
        let mut cache = self.quote_tokens.write().await;
        cache.fetched_ms = now_ts_ms();
        cache.tokens = tokens;
        Ok(())
    }

    async fn request_json_with_base(
        &self,
        base_url: &str,
        method: Method,
        path: &str,
        payload: Option<Value>,
        params: Option<&[(&str, String)]>,
    ) -> Result<Value> {
        self.request_json_with_base_raw(base_url, method, path, payload, params)
            .await
            .map(|(parsed, _raw)| parsed)
    }

    async fn request_json_with_base_raw(
        &self,
        base_url: &str,
        method: Method,
        path: &str,
        payload: Option<Value>,
        params: Option<&[(&str, String)]>,
    ) -> Result<(Value, String)> {
        let payload_log = payload.as_ref().map(|p| p.to_string());
        let payload_out = payload_log.clone().unwrap_or_else(|| "null".to_string());
        let params_log = params.map(|entries| {
            entries
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&")
        });
        let mut url = Url::parse(&format!(
            "{}/{}",
            base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        ))
        .context("opinion openapi url parse failed")?;
        if let Some(params) = params {
            let mut qp = url.query_pairs_mut();
            for (key, value) in params {
                qp.append_pair(key, value);
            }
        }
        let mut req = self
            .client
            .request(method.clone(), url.clone())
            .header("apikey", &self.api_key)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, OPINION_UA);
        if let Some(payload) = payload {
            if method != Method::GET && method != Method::DELETE {
                req = req.header("Content-Type", "application/json").json(&payload);
            }
        }
        let resp = req.send().await.context("opinion openapi request failed")?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let text = resp.text().await.unwrap_or_default();
        let request_id = extract_request_id(&headers);
        if !status.is_success() {
            let apikey_masked = mask_key(&self.api_key);
            warn!(
                "opinion_openapi http_error method={} url={} status={} apikey={} user_agent={} body_prefix=\"{}\"",
                method,
                url,
                status,
                apikey_masked,
                OPINION_UA,
                text.chars().take(200).collect::<String>()
            );
            if is_order_endpoint(path) {
                warn!(
                    "opinion_order_response result=http_error method={} path={} status={} payload={} response_raw={}",
                    method,
                    path,
                    status,
                    payload_out,
                    text
                );
            }
            log_request_id_sample(
                &method,
                &url,
                Some(status),
                request_id.as_deref(),
                &text,
            );
            return Err(anyhow!(
                "opinion openapi http error method={} url={} status={} body={}",
                method,
                url,
                status,
                text
            ));
        }
        let raw = text;
        let value: Value = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(err) => {
                if is_order_endpoint(path) {
                    warn!(
                        "opinion_order_response result=parse_error method={} path={} status={} payload={} response_raw={}",
                        method,
                        path,
                        status,
                        payload_out,
                        raw
                    );
                }
                return Err(anyhow!(err).context(format!(
                    "opinion openapi response parse failed: {}",
                    raw
                )));
            }
        };
        match parse_result(value) {
            Ok(parsed) => {
                if is_order_endpoint(path) {
                    tracing::info!(
                        "opinion_order_response result=ok method={} path={} status={} payload={} response_raw={}",
                        method,
                        path,
                        status,
                        payload_out,
                        raw
                    );
                }
                Ok((parsed, raw))
            }
            Err(err) => {
                if is_order_endpoint(path) {
                    warn!(
                        "opinion_order_response result=api_error method={} path={} status={} payload={} response_raw={} err={}",
                        method,
                        path,
                        status,
                        payload_out,
                        raw,
                        err
                    );
                } else {
                    let apikey_masked = mask_key(&self.api_key);
                    let params_out = params_log.as_deref().unwrap_or("");
                    warn!(
                        "opinion_openapi api_error method={} url={} params=\"{}\" payload={} apikey={} user_agent={} err={} response={}",
                        method,
                        url,
                        params_out,
                        payload_out,
                        apikey_masked,
                        OPINION_UA,
                        err,
                        raw
                    );
                }
                log_request_id_sample(
                    &method,
                    &url,
                    Some(status),
                    request_id.as_deref(),
                    &raw,
                );
                Err(err)
            }
        }
    }

}

#[derive(Debug, Clone)]
struct OrderPayload {
    salt: U256,
    maker: Address,
    signer: Address,
    taker: Address,
    token_id: U256,
    maker_amount: U256,
    taker_amount: U256,
    expiration: U256,
    nonce: U256,
    fee_rate_bps: U256,
    side: u8,
    signature_type: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn as_int(self) -> i32 {
        match self {
            OrderSide::Buy => 0,
            OrderSide::Sell => 1,
        }
    }
}

pub fn build_opinion_order_map(pairs: &[PairRecord]) -> HashMap<(i64, TokenSide), OpinionOrderTarget> {
    let mut map = HashMap::new();
    for pair in pairs {
        let market_id = match pair.opinion_market_id.as_ref() {
            Some(value) => match value.parse::<i64>() {
                Ok(v) => v,
                Err(_) => {
                    warn!("trade: invalid opinion_market_id pair_id={} market_id={}", pair.pair_id, value);
                    continue;
                }
            },
            None => continue,
        };
        if let Some(token_id) = pair.opinion_yes_token_id.as_ref() {
            map.insert(
                (pair.pair_id, TokenSide::Yes),
                OpinionOrderTarget {
                    market_id,
                    token_id: token_id.clone(),
                },
            );
        }
        if let Some(token_id) = pair.opinion_no_token_id.as_ref() {
            map.insert(
                (pair.pair_id, TokenSide::No),
                OpinionOrderTarget {
                    market_id,
                    token_id: token_id.clone(),
                },
            );
        }
    }
    map
}

pub fn build_opinion_client(cfg: &TradeConfig) -> Result<OpinionPrivateClient> {
    let account = load_opinion_account(&cfg.opinion_account_file, &cfg.opinion_account_id)?;
    OpinionPrivateClient::new(
        cfg.opinion_private_base.clone(),
        account.api_key,
        account.private_key,
        account.multi_sig_address,
        account.multisend_address,
        account.chain_id,
        account.rpc_url,
        account.conditional_tokens_address,
        cfg.opinion_check_approval,
        cfg.opinion_enable_retry_ms,
        cfg.opinion_force_eoa_orders,
    )
}

#[derive(Debug, Deserialize)]
struct AccountsFile {
    accounts: Vec<AccountEntry>,
}

#[derive(Debug, Deserialize)]
struct AccountEntry {
    account_id: Option<String>,
    exchange: Option<String>,
    api_key: Option<String>,
    private_key: Option<String>,
    multi_sig_address: Option<String>,
    multisend_address: Option<String>,
    rpc_url: Option<String>,
    conditional_tokens_address: Option<String>,
    chain_id: Option<u64>,
}

struct OpinionAccount {
    api_key: String,
    private_key: String,
    multi_sig_address: Option<String>,
    multisend_address: Option<String>,
    rpc_url: Option<String>,
    conditional_tokens_address: Option<String>,
    chain_id: u64,
}

fn load_opinion_account(path: &str, account_id: &str) -> Result<OpinionAccount> {
    if !std::path::Path::new(path).exists() {
        return Err(anyhow!("opinion account file missing: {}", path));
    }
    let raw = std::fs::read_to_string(path).context("read opinion account file failed")?;
    let parsed: AccountsFile = serde_json::from_str(&raw).context("parse opinion accounts failed")?;
    let mut fallback: Option<OpinionAccount> = None;
    for account in parsed.accounts {
        if let Some(exchange) = account.exchange.as_ref() {
            if !exchange.eq_ignore_ascii_case("Opinion") {
                continue;
            }
        }
        let api_key = account
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string);
        let private_key = account
            .private_key
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string);
        if api_key.is_none() || private_key.is_none() {
            continue;
        }
        let candidate = OpinionAccount {
            api_key: api_key.unwrap(),
            private_key: private_key.unwrap(),
            multi_sig_address: account
                .multi_sig_address
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string),
            multisend_address: account
                .multisend_address
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string),
            rpc_url: account
                .rpc_url
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string),
            conditional_tokens_address: account
                .conditional_tokens_address
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string),
            chain_id: account.chain_id.unwrap_or(56),
        };
        let matches_id = account
            .account_id
            .as_deref()
            .map(|id| id == account_id)
            .unwrap_or(false);
        if matches_id || account_id.is_empty() {
            return Ok(candidate);
        }
        fallback = Some(candidate);
    }
    fallback.ok_or_else(|| anyhow!("opinion account not found for {}", account_id))
}

#[derive(Debug)]
struct MultiSendTx {
    operation: u8,
    to: Address,
    value: U256,
    data: Bytes,
}

fn min_threshold(decimals: u8) -> U256 {
    let base = U256::from(1_000_000_000u64);
    let mut power = U256::one();
    for _ in 0..decimals {
        power = power
            .checked_mul(U256::from(10u64))
            .unwrap_or(U256::MAX);
    }
    base.checked_mul(power).unwrap_or(U256::MAX)
}

fn encode_multisend_txs(txs: &[MultiSendTx]) -> Result<Bytes> {
    let mut out = Vec::new();
    for tx in txs {
        out.push(tx.operation);
        out.extend_from_slice(tx.to.as_bytes());
        let mut value_bytes = [0u8; 32];
        tx.value.to_big_endian(&mut value_bytes);
        out.extend_from_slice(&value_bytes);
        let mut len_bytes = [0u8; 32];
        U256::from(tx.data.len()).to_big_endian(&mut len_bytes);
        out.extend_from_slice(&len_bytes);
        out.extend_from_slice(&tx.data);
    }
    Ok(Bytes::from(out))
}

fn normalize_private_key(key: &str) -> String {
    let trimmed = key.trim();
    if trimmed.starts_with("0x") {
        trimmed.to_string()
    } else {
        format!("0x{}", trimmed)
    }
}

fn ensure_0x(value: String) -> String {
    if value.starts_with("0x") {
        value
    } else {
        format!("0x{}", value)
    }
}

fn parse_address(value: &str) -> Result<Address> {
    Address::from_str(value.trim()).context("invalid address")
}

fn format_price_checked(value: f64) -> Result<String> {
    let rounded_str = format!("{:.6}", value);
    let rounded = rounded_str
        .parse::<f64>()
        .unwrap_or(value);
    if (value - rounded).abs() > 1e-9 {
        return Err(anyhow!(
            "Price precision cannot exceed 6 decimal places: {}",
            value
        ));
    }
    Ok(format!("{:.6}", rounded))
}

fn validate_price(value: f64) -> Result<String> {
    if !value.is_finite() {
        return Err(anyhow!("price must be finite"));
    }
    if value < 0.001 || value > 0.999 {
        return Err(anyhow!(
            "Price must be between 0.001 and 0.999 (inclusive), got {}",
            value
        ));
    }
    format_price_checked(value)
}

fn decimal_from_f64(value: f64) -> Result<Decimal> {
    if !value.is_finite() {
        return Err(anyhow!("invalid decimal value {}", value));
    }
    let mut s = format!("{:.18}", value);
    if let Some(dot) = s.find('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.truncate(dot);
        }
    }
    Decimal::from_str(&s).context("invalid decimal format")
}

fn compute_maker_amount(side: OrderSide, amount: OrderAmount, price_str: &str) -> Result<Decimal> {
    let price = Decimal::from_str(price_str).context("invalid price")?;
    let minimal = Decimal::ONE;
    let maker_amount = match side {
        OrderSide::Buy => match amount {
            OrderAmount::Base(value) => {
                let base = decimal_from_f64(value)?;
                if base < minimal {
                    return Err(anyhow!("makerAmountInBaseToken must be at least 1"));
                }
                base * price
            }
            OrderAmount::Quote(value) => {
                let quote = decimal_from_f64(value)?;
                if quote < minimal {
                    return Err(anyhow!("makerAmountInQuoteToken must be at least 1"));
                }
                quote
            }
        },
        OrderSide::Sell => match amount {
            OrderAmount::Base(value) => {
                let base = decimal_from_f64(value)?;
                if base < minimal {
                    return Err(anyhow!("makerAmountInBaseToken must be at least 1"));
                }
                base
            }
            OrderAmount::Quote(value) => {
                let quote = decimal_from_f64(value)?;
                if quote < minimal {
                    return Err(anyhow!("makerAmountInQuoteToken must be at least 1"));
                }
                if price.is_zero() {
                    return Err(anyhow!(
                        "Price cannot be zero for SELL orders with makerAmountInQuoteToken"
                    ));
                }
                quote / price
            }
        },
    };
    if maker_amount <= Decimal::ZERO {
        return Err(anyhow!(
            "Calculated makerAmount must be positive, got {}",
            maker_amount
        ));
    }
    Ok(maker_amount)
}

fn safe_amount_to_wei(amount: Decimal, decimals: u32) -> Result<U256> {
    if amount <= Decimal::ZERO {
        return Err(anyhow!("Amount must be positive, got {}", amount));
    }
    if decimals > MAX_DECIMALS {
        return Err(anyhow!(
            "Decimals must be between 0 and {}, got {}",
            MAX_DECIMALS,
            decimals
        ));
    }
    let multiplier = Decimal::from_i128_with_scale(10i128.pow(decimals), 0);
    let result_decimal = (amount * multiplier).trunc();
    if result_decimal <= Decimal::ZERO {
        return Err(anyhow!(
            "Calculated amount is zero or negative: {}",
            result_decimal
        ));
    }
    let mut result_str = result_decimal.to_string();
    if let Some(dot) = result_str.find('.') {
        result_str.truncate(dot);
    }
    let result = U256::from_dec_str(&result_str)
        .context("Amount too large for uint256 or invalid")?;
    if result.is_zero() {
        return Err(anyhow!("Calculated amount is zero"));
    }
    Ok(result)
}

fn generate_salt() -> U256 {
    let now_seconds = now_ts_ms() as f64 / 1000.0;
    let rand = rand::thread_rng().gen::<f64>();
    let salt = (now_seconds * rand).round();
    if salt.is_finite() && salt > 0.0 {
        U256::from(salt as u64)
    } else {
        U256::from(1u64)
    }
}

fn price_to_fraction(price: &str) -> Result<(u64, u64)> {
    let price_decimal = Decimal::from_str(price).context("invalid price")?;
    let min_price = Decimal::from_str("0.001").unwrap_or(Decimal::new(1, 3));
    let max_price = Decimal::from_str("0.999").unwrap_or(Decimal::new(999, 3));
    if price_decimal < min_price || price_decimal > max_price {
        return Err(anyhow!(
            "Price must be between {} and {} (inclusive), got {}",
            min_price,
            max_price,
            price
        ));
    }
    if price_decimal.scale() > 6 {
        return Err(anyhow!(
            "Price precision cannot exceed 6 decimal places, got {}",
            price
        ));
    }
    let mut numerator = 0u64;
    let mut denominator = 1u64;
    let parts: Vec<&str> = price.split('.').collect();
    if parts.len() == 1 {
        numerator = parts[0].parse::<u64>().context("invalid price numerator")?;
        denominator = 1;
    } else if parts.len() == 2 {
        let whole = parts[0].parse::<u64>().context("invalid price whole")?;
        let frac = parts[1];
        let pow = 10u64.pow(frac.len() as u32);
        let frac_val = if frac.is_empty() {
            0u64
        } else {
            frac.parse::<u64>().context("invalid price fraction")?
        };
        numerator = whole
            .checked_mul(pow)
            .and_then(|v| v.checked_add(frac_val))
            .context("price overflow")?;
        denominator = pow;
    }
    if denominator == 0 {
        return Err(anyhow!("invalid price denominator"));
    }
    let g = gcd(numerator, denominator);
    Ok((numerator / g, denominator / g))
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let tmp = a % b;
        a = b;
        b = tmp;
    }
    if a == 0 { 1 } else { a }
}

fn round_to_significant_digits(value: U256, n: usize) -> U256 {
    if value.is_zero() {
        return U256::zero();
    }
    let magnitude = value.to_string().len();
    if magnitude <= n {
        return value;
    }
    let power = magnitude - n;
    let mut divisor = U256::one();
    for _ in 0..power {
        divisor = divisor.saturating_mul(U256::from(10u8));
    }
    let quotient = value / divisor;
    let remainder = value % divisor;
    let twice = remainder.saturating_mul(U256::from(2u8));
    let mut rounded = quotient;
    if twice > divisor {
        rounded = quotient.saturating_add(U256::one());
    } else if twice == divisor {
        if (quotient & U256::one()) == U256::one() {
            rounded = quotient.saturating_add(U256::one());
        }
    }
    rounded.saturating_mul(divisor)
}

fn calculate_order_amounts(
    price: &str,
    maker_amount: U256,
    side: OrderSide,
) -> Result<(U256, U256)> {
    let (price_num, price_denom) = price_to_fraction(price)?;
    let maker_4digit = round_to_significant_digits(maker_amount, 4);
    let num = U256::from(price_num);
    let denom = U256::from(price_denom);
    let mut k = match side {
        OrderSide::Buy => maker_4digit / num,
        OrderSide::Sell => maker_4digit / denom,
    };
    if k.is_zero() {
        k = U256::one();
    }
    let (mut recalculated_maker_amount, mut taker_amount) = match side {
        OrderSide::Buy => (k.saturating_mul(num), k.saturating_mul(denom)),
        OrderSide::Sell => (k.saturating_mul(denom), k.saturating_mul(num)),
    };
    if recalculated_maker_amount.is_zero() {
        recalculated_maker_amount = U256::one();
    }
    if taker_amount.is_zero() {
        taker_amount = U256::one();
    }
    let calculated_price = price_num as f64 / price_denom as f64;
    if calculated_price < 0.001 || calculated_price > 0.999 {
        return Err(anyhow!("invalid taker_amount and recalculated_maker_amount"));
    }
    Ok((recalculated_maker_amount, taker_amount))
}

pub fn round_price_for_side(price: f64, side: OrderSide) -> f64 {
    let factor = 1000.0;
    if !price.is_finite() {
        return price;
    }
    match side {
        OrderSide::Buy => (price * factor).floor() / factor,
        OrderSide::Sell => (price * factor).ceil() / factor,
    }
}

fn parse_result(value: Value) -> Result<Value> {
    if let Some(errno) = value.get("errno").and_then(|v| v.as_i64()) {
        if errno != 0 {
            let msg = value
                .get("errmsg")
                .and_then(|v| v.as_str())
                .unwrap_or("opinion error");
            return Err(anyhow!("opinion api error errno={} errmsg={}", errno, msg));
        }
    }
    if let Some(result) = value.get("result") {
        return Ok(result.clone());
    }
    if let Some(data) = value.get("data") {
        return Ok(data.clone());
    }
    Ok(value)
}

fn is_errno(err: &anyhow::Error, code: i64) -> bool {
    let msg = err.to_string();
    let key = "errno=";
    if let Some(idx) = msg.find(key) {
        let start = idx + key.len();
        let bytes = msg.as_bytes();
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end > start {
            if let Ok(parsed) = msg[start..end].parse::<i64>() {
                return parsed == code;
            }
        }
    }
    false
}

fn extract_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(val) = value.get(*key) {
            if let Some(s) = val.as_str() {
                return Some(s.to_string());
            }
            if let Some(n) = val.as_i64() {
                return Some(n.to_string());
            }
            if let Some(n) = val.as_u64() {
                return Some(n.to_string());
            }
        }
    }
    None
}

fn extract_string_nested(value: &Value, keys: &[&str]) -> Option<String> {
    if let Some(found) = extract_string(value, keys) {
        return Some(found);
    }
    for container in ["result", "data", "order", "order_info", "orderInfo"] {
        if let Some(val) = value.get(container) {
            if let Some(found) = extract_string(val, keys) {
                return Some(found);
            }
            if let Some(nested) = val.get("data") {
                if let Some(found) = extract_string(nested, keys) {
                    return Some(found);
                }
            }
        }
    }
    None
}

fn extract_order_id(value: &Value) -> Option<String> {
    if let Some(order_data) = value
        .get("orderData")
        .or_else(|| value.get("order_data"))
    {
        if let Some(found) = extract_string(order_data, &["orderId", "order_id", "id"]) {
            return Some(found);
        }
    }
    for container in ["result", "data"] {
        if let Some(val) = value.get(container) {
            if let Some(order_data) = val
                .get("orderData")
                .or_else(|| val.get("order_data"))
            {
                if let Some(found) = extract_string(order_data, &["orderId", "order_id", "id"]) {
                    return Some(found);
                }
            }
            if let Some(found) = extract_string(val, &["orderId", "order_id", "id"]) {
                return Some(found);
            }
        }
    }
    extract_string_nested(value, &["orderId", "order_id", "id"])
}

fn extract_client_order_id(value: &Value) -> Option<String> {
    extract_string(
        value,
        &["clientOrderId", "client_order_id", "clientId", "client_id"],
    )
}

fn extract_ts_ms(value: &Value) -> Option<i64> {
    if let Some(ts) = value.get("ts_ms").and_then(|v| v.as_i64()) {
        return Some(ts);
    }
    if let Some(ts) = value.get("timestamp").and_then(|v| v.as_i64()) {
        return Some(ts);
    }
    if let Some(ts) = value.get("time").and_then(|v| v.as_i64()) {
        return Some(ts);
    }
    None
}

fn extract_i64_from_value(value: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(val) = value.get(*key) {
            if let Some(n) = val.as_i64() {
                return Some(n);
            }
            if let Some(n) = val.as_u64() {
                return Some(n as i64);
            }
            if let Some(s) = val.as_str() {
                if let Ok(parsed) = s.parse::<i64>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

fn extract_created_ms(value: &Value) -> Option<i64> {
    let raw = extract_i64_from_value(value, &["createdAt", "created_at", "createTime", "create_time"])?;
    if raw > 0 && raw < 1_000_000_000_000 {
        return Some(raw.saturating_mul(1000));
    }
    Some(raw)
}

fn order_matches_attributes(
    value: &Value,
    market_id: i64,
    expected_side: i64,
    expected_price: Option<f64>,
) -> bool {
    if let Some(entry_market) = extract_i64_from_value(value, &["marketId", "topicId", "market_id"]) {
        if entry_market != market_id {
            return false;
        }
    }
    if let Some(entry_side) = extract_i64_from_value(value, &["side", "orderSide", "sideEnum"]) {
        if entry_side != expected_side {
            return false;
        }
    }
    if let (Some(entry_price), Some(expected_price)) =
        (extract_f64_from_value(value, &["price"]), expected_price)
    {
        if (entry_price - expected_price).abs() > 1e-6 {
            return false;
        }
    }
    true
}

fn extract_f64_from_value(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(val) = value.get(*key) {
            if let Some(n) = val.as_f64() {
                return Some(n);
            }
            if let Some(s) = val.as_str() {
                if let Ok(parsed) = s.parse::<f64>() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}
