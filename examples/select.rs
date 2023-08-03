use anyhow::Result;
use libsql_client::{args, de, Client, Statement};
use rand::prelude::SliceRandom;

#[derive(Debug, serde::Deserialize)]
struct Counter {
    country: String,
    city: String,
    value: i64,
}

fn result_to_string(counters: Vec<Counter>) -> Result<String> {
    let mut ret = String::new();

    let columns = ["country", "city", "value"];

    for column in &columns {
        ret += &format!("| {:16} |", column);
    }
    ret += "\n| -------------------------------------------------------- |\n";
    for row in counters {
        let values = [row.country, row.city, row.value.to_string()];
        for cell in values {
            ret += &format!("| {:16} |", cell);
        }
        ret += "\n";
    }
    Ok(ret)
}

// Bumps a counter for one of the geographic locations picked at random.
async fn bump_counter(db: Client) -> Result<String> {
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

    let (airport, country, city, latitude, longitude) =
        *FAKE_LOCATIONS.choose(&mut rand::thread_rng()).unwrap();

    db.batch([
        Statement::with_args(
            "INSERT OR IGNORE INTO counter VALUES (?, ?, 0)",
            &[country, city],
        ),
        Statement::with_args(
            "UPDATE counter SET value = value + 1 WHERE country = ? AND city = ?",
            &[country, city],
        ),
        Statement::with_args(
            "INSERT OR IGNORE INTO coordinates VALUES (?, ?, ?)",
            args!(latitude, longitude, airport),
        ),
    ])
    .await?;

    let counter_response = db
        .execute("SELECT * FROM counter")
        .await?
        .rows
        .iter()
        .map(de::from_row)
        .collect::<Result<Vec<Counter>, _>>()?;

    let scoreboard = result_to_string(counter_response)?;
    let html = format!("Scoreboard:\n{scoreboard}");
    Ok(html)
}

#[tokio::main]
async fn main() {
    match libsql_client::Client::from_env().await {
        Ok(client) => {
            match bump_counter(client).await {
                Ok(response) => println!("Response:\n{response}"),
                Err(e) => println!("Remote database query failed: {e}"),
            };
        }
        Err(e) => println!("Failed to fetch from a remote database: {e}"),
    }
}
