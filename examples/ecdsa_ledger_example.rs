//! ECDSA signed transaction ledger example
//!
//! Demonstrates cryptographic primitives end-to-end: keygen, sign transfers,
//! verify, propagate over network.

use chaincraft::{
    crypto::ecdsa::ECDSASigner,
    error::Result,
    examples::ecdsa_ledger::{helpers, ECDSALedgerNode},
};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("Chaincraft ECDSA Ledger Example");
    println!("===============================\n");

    let mut node = ECDSALedgerNode::new(0).await?;
    node.start().await?;

    let alice = ECDSASigner::new()?;
    let bob = ECDSASigner::new()?;
    let alice_pk = alice.get_public_key_pem()?;
    let bob_pk = bob.get_public_key_pem()?;

    // Alice "mints" 100 (first tx from new address)
    let tx1 = helpers::create_transfer(alice_pk.clone(), bob_pk.clone(), 50, 0, &alice)?;
    node.publish(tx1).await?;
    println!("Alice -> Bob: 50 (first tx = mint)");
    sleep(Duration::from_millis(200)).await;

    // Bob -> Alice: 20
    let tx2 = helpers::create_transfer(bob_pk.clone(), alice_pk.clone(), 20, 0, &bob)?;
    node.publish(tx2).await?;
    println!("Bob -> Alice: 20");
    sleep(Duration::from_millis(200)).await;

    println!("\nLedger entries: {}", node.entry_count().await?);
    println!("Alice balance: {}", node.balance(&alice_pk).await?);
    println!("Bob balance: {}", node.balance(&bob_pk).await?);

    node.close().await?;
    println!("Done.");
    Ok(())
}
