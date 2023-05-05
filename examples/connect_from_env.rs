use anyhow::Result;
use libsql_client::{args, new_client, DatabaseClient, ResultSet, Statement};
use rand::prelude::SliceRandom;

fn result_to_string(query_result: ResultSet) -> Result<String> {
    let mut ret = String::new();
    let ResultSet { columns, rows, .. } = query_result;
    for column in &columns {
        ret += &format!("| {:16} |", column);
    }
    ret += "\n| -------------------------------------------------------- |\n";
    for row in rows {
        for cell in row.values {
            ret += &format!("| {:16} |", cell);
        }
        ret += "\n";
    }
    Ok(ret)
}

// Bumps a counter for one of the geographic locations picked at random.
async fn bump_counter(db: impl DatabaseClient) -> Result<String> {
    // Recreate the tables if they do not exist yet
    db.batch([
        "CREATE TABLE IF NOT EXISTS counter(country TEXT, city TEXT, value, PRIMARY KEY(country, city)) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS coordinates(lat INT, long INT, airport TEXT, PRIMARY KEY (lat, long))"
    ]).await?;

    // For demo purposes, let's pick a pseudorandom location
    const FAKE_LOCATIONS: &[(&str, &str, &str, f64, f64)] = &[
        ("WAW", "PL", "Warsaw", 52.22959, 21.0067),
        ("EWR", "US", "Newark", 42.99259, -81.3321),
        ("HAM", "DE", "Hamburg", 50.118801, 7.684300),
        ("HEL", "FI", "Helsinki", 60.3183, 24.9497),
        ("NSW", "AU", "Sydney", -33.9500, 151.1819),
    ];

    for _ in 0..3 {
        let (airport, country, city, latitude, longitude) =
            *FAKE_LOCATIONS.choose(&mut rand::thread_rng()).unwrap();
        let transaction = db.transaction().await?;
        transaction
            .execute(Statement::with_args(
                "INSERT OR IGNORE INTO counter VALUES (?, ?, 0)",
                // Parameters that have a single type can be passed as a regular slice
                &[country, city],
            ))
            .await?;
        transaction
            .execute(Statement::with_args(
                "UPDATE counter SET value = value + 1 WHERE country = ? AND city = ?",
                &[country, city],
            ))
            .await?;
        transaction
            .execute(Statement::with_args(
                "INSERT OR IGNORE INTO coordinates VALUES (?, ?, ?)",
                // Parameters with different types can be passed to a convenience macro - args!()
                args!(latitude, longitude, airport),
            ))
            .await?;
        transaction.commit().await?;
    }

    /* NOTICE: interactive transactions only work with WebSocket and local backends. For HTTP, use batches:
        db.batch([
            Statement::with_args("INSERT OR IGNORE INTO counter VALUES (?, ?, 0)", &[country, city]),
            Statement::with_args("UPDATE counter SET value = value + 1 WHERE country = ? AND city = ?", &[country, city]),
            Statement::with_args("INSERT OR IGNORE INTO coordinates VALUES (?, ?, ?)", args!(latitude, longitude, airport)),
        ]).await?;
    */

    let counter_response = db.execute("SELECT * FROM counter").await?;
    let scoreboard = result_to_string(counter_response)?;
    let html = format!("Scoreboard:\n{scoreboard}");
    Ok(html)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let db = new_client().await.unwrap();
    let response = bump_counter(db)
        .await
        .unwrap_or_else(|e| format!("Error: {e}"));
    println!(
        "Client parameters: backend={:?} url={:?} token={:?}\n{response}",
        std::env::var("LIBSQL_CLIENT_BACKEND"),
        std::env::var("LIBSQL_CLIENT_URL"),
        std::env::var("LIBSQL_CLIENT_TOKEN"),
    );
}
