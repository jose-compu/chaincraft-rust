//! Randomness Beacon Example
//!
//! Demonstrates the verifiable randomness beacon protocol.
//! Ported from the Python chaincraft examples (randomness_beacon.py).
//!
//! Features:
//! - Validator registration
//! - VRF proof submission
//! - Threshold-based randomness finalization
//!
//! Run with: `cargo run --example randomness_beacon_example`

use chaincraft::{
    error::Result,
    examples::randomness_beacon::{helpers, RandomnessBeaconNode},
};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("Chaincraft Randomness Beacon Example");
    println!("=====================================\n");

    let mut node = RandomnessBeaconNode::new(0, 60, 2).await?;
    node.start().await?;
    let my_address = node.validator_address().await?;

    println!(
        "Node started. Validator address: {}...",
        &my_address[..my_address.len().min(40)]
    );

    let reg_signer = chaincraft::crypto::ecdsa::ECDSASigner::new()?;
    let reg_msg = helpers::create_validator_registration(
        my_address.clone(),
        reg_signer.get_public_key_pem()?,
        "vrf_key_placeholder".to_string(),
        100,
        &reg_signer,
    )?;

    node.publish(reg_msg).await?;
    println!("Validator registration sent");
    sleep(Duration::from_millis(200)).await;

    let state = node.beacon_state().await?;
    println!("\nBeacon state: {}", serde_json::to_string_pretty(&state).unwrap());

    println!("\nShutting down...");
    node.close().await?;
    println!("Done.");

    Ok(())
}
