use crate::models::{PairRecord, TokenRegistration, TokenSide, Venue};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct RegistryState {
    pub token_pairs: HashMap<String, SmallVec<[i64; 4]>>,
    pub token_sides: HashMap<String, TokenSide>,
    pub opi_market_pairs: HashMap<String, SmallVec<[i64; 4]>>,
    pub pm_market_pairs: HashMap<String, SmallVec<[i64; 4]>>,
    pub opi_market_side_key: HashMap<(String, i32), String>,
    pub opi_expected_token_id: HashMap<(String, i32), String>,
    pub pm_asset_key: HashMap<String, String>,
    pub opi_market_ids: HashSet<String>,
    pub pm_asset_ids: HashSet<String>,
}

#[derive(Debug, Default)]
pub struct TokenRegistry {
    pub tokens: Vec<TokenRegistration>,
    pub state: RegistryState,
}

impl TokenRegistry {
    pub fn build(pairs: &[PairRecord]) -> Self {
        let mut registry = TokenRegistry::default();
        for pair in pairs {
            let opinion_market_id = pair.opinion_market_id.clone();
            let polymarket_market_id = pair.polymarket_market_id.clone();

            if let Some(op_market_id) = &opinion_market_id {
                registry.state.opi_market_ids.insert(op_market_id.clone());
                let entry = registry
                    .state
                    .opi_market_pairs
                    .entry(op_market_id.clone())
                    .or_insert_with(SmallVec::new);
                if !entry.contains(&pair.pair_id) {
                    entry.push(pair.pair_id);
                }
            }
            if let Some(pm_market_id) = &polymarket_market_id {
                let entry = registry
                    .state
                    .pm_market_pairs
                    .entry(pm_market_id.clone())
                    .or_insert_with(SmallVec::new);
                if !entry.contains(&pair.pair_id) {
                    entry.push(pair.pair_id);
                }
            }

            let (op_yes_key, op_no_key) = opinion_token_keys(pair);
            let (pm_yes_key, pm_no_key) = polymarket_token_keys(pair);

            registry.register_token(
                op_yes_key.clone(),
                Venue::Opinion,
                pair.opinion_yes_token_id.clone(),
                opinion_market_id.clone(),
                Some(1),
                TokenSide::Yes,
                pair.pair_id,
            );
            registry.state.opi_market_side_key.insert(
                (opinion_market_id.clone().unwrap_or_default(), 1),
                op_yes_key.clone(),
            );
            if let Some(token_id) = &pair.opinion_yes_token_id {
                registry.state.opi_expected_token_id.insert(
                    (opinion_market_id.clone().unwrap_or_default(), 1),
                    token_id.clone(),
                );
            }

            registry.register_token(
                op_no_key.clone(),
                Venue::Opinion,
                pair.opinion_no_token_id.clone(),
                opinion_market_id.clone(),
                Some(2),
                TokenSide::No,
                pair.pair_id,
            );
            registry.state.opi_market_side_key.insert(
                (opinion_market_id.clone().unwrap_or_default(), 2),
                op_no_key.clone(),
            );
            if let Some(token_id) = &pair.opinion_no_token_id {
                registry.state.opi_expected_token_id.insert(
                    (opinion_market_id.clone().unwrap_or_default(), 2),
                    token_id.clone(),
                );
            }

            registry.register_token(
                pm_yes_key.clone(),
                Venue::Polymarket,
                pair.polymarket_yes_token_id.clone(),
                pair.polymarket_market_id.clone(),
                None,
                TokenSide::Yes,
                pair.pair_id,
            );
            registry.register_token(
                pm_no_key.clone(),
                Venue::Polymarket,
                pair.polymarket_no_token_id.clone(),
                pair.polymarket_market_id.clone(),
                None,
                TokenSide::No,
                pair.pair_id,
            );

            if let Some(yes_asset) = &pair.polymarket_yes_token_id {
                registry.state.pm_asset_ids.insert(yes_asset.clone());
                registry
                    .state
                    .pm_asset_key
                    .insert(yes_asset.clone(), pm_yes_key);
            }
            if let Some(no_asset) = &pair.polymarket_no_token_id {
                registry.state.pm_asset_ids.insert(no_asset.clone());
                registry
                    .state
                    .pm_asset_key
                    .insert(no_asset.clone(), pm_no_key);
            }
        }
        registry
    }

    fn register_token(
        &mut self,
        token_key: String,
        venue: Venue,
        external_token_id: Option<String>,
        market_id: Option<String>,
        outcome_side: Option<i32>,
        token_side: TokenSide,
        pair_id: i64,
    ) {
        let entry = self
            .state
            .token_pairs
            .entry(token_key.clone())
            .or_insert_with(SmallVec::new);
        if !entry.contains(&pair_id) {
            entry.push(pair_id);
        }
        self.state.token_sides.insert(token_key.clone(), token_side);
        self.tokens.push(TokenRegistration {
            token_key,
            venue,
            external_token_id,
            market_id,
            outcome_side,
            token_side,
            pair_ids: SmallVec::new(),
        });
    }
}

impl RegistryState {
    pub fn token_side_for_opi(outcome_side: i32) -> TokenSide {
        if outcome_side == 1 {
            TokenSide::Yes
        } else if outcome_side == 2 {
            TokenSide::No
        } else {
            TokenSide::Unknown
        }
    }

    pub fn ensure_opi_token(
        &mut self,
        market_id: &str,
        outcome_side: i32,
        token_id: &str,
    ) -> (String, bool, bool, SmallVec<[i64; 4]>) {
        let token_key = format!("opi:token:{token_id}");
        let key = (market_id.to_string(), outcome_side);
        let prev = self.opi_expected_token_id.get(&key);
        let is_new = prev.is_none();
        let mismatch = prev.map(|v| v != token_id).unwrap_or(false);
        self.opi_expected_token_id.insert(key, token_id.to_string());
        self.opi_market_side_key
            .insert((market_id.to_string(), outcome_side), token_key.clone());
        self.token_sides
            .insert(token_key.clone(), Self::token_side_for_opi(outcome_side));
        let pairs = self
            .opi_market_pairs
            .get(market_id)
            .cloned()
            .unwrap_or_else(SmallVec::new);
        if !pairs.is_empty() {
            let entry = self
                .token_pairs
                .entry(token_key.clone())
                .or_insert_with(SmallVec::new);
            for pair_id in &pairs {
                if !entry.contains(pair_id) {
                    entry.push(*pair_id);
                }
            }
        }
        (token_key, mismatch, is_new, pairs)
    }

    pub fn token_key_for_opi_market(&self, market_id: &str, outcome_side: i32) -> Option<String> {
        self.opi_market_side_key
            .get(&(market_id.to_string(), outcome_side))
            .cloned()
    }
}

pub fn token_key_opinion(
    token_id: Option<&str>,
    market_id: Option<&str>,
    outcome_side: i32,
) -> String {
    if let Some(token_id) = token_id {
        if !token_id.trim().is_empty() {
            return format!("opi:token:{token_id}");
        }
    }
    let market_id = market_id.unwrap_or("unknown");
    format!("opi:market:{market_id}:{outcome_side}")
}

pub fn token_key_polymarket(
    token_id: Option<&str>,
    market_id: Option<&str>,
    side: TokenSide,
) -> String {
    if let Some(token_id) = token_id {
        if !token_id.trim().is_empty() {
            return format!("pm:asset:{token_id}");
        }
    }
    let market_id = market_id.unwrap_or("unknown");
    format!("pm:market:{market_id}:{}", side.as_str())
}

fn opinion_token_keys(pair: &PairRecord) -> (String, String) {
    let yes_key = token_key_opinion(
        pair.opinion_yes_token_id.as_deref(),
        pair.opinion_market_id.as_deref(),
        1,
    );
    let no_key = token_key_opinion(
        pair.opinion_no_token_id.as_deref(),
        pair.opinion_market_id.as_deref(),
        2,
    );
    (yes_key, no_key)
}

fn polymarket_token_keys(pair: &PairRecord) -> (String, String) {
    let yes_key = token_key_polymarket(
        pair.polymarket_yes_token_id.as_deref(),
        pair.polymarket_market_id.as_deref(),
        TokenSide::Yes,
    );
    let no_key = token_key_polymarket(
        pair.polymarket_no_token_id.as_deref(),
        pair.polymarket_market_id.as_deref(),
        TokenSide::No,
    );
    (yes_key, no_key)
}
