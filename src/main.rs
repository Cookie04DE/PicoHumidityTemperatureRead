use std::{io::ErrorKind, process::ExitCode};

use anyhow::anyhow;
use chrono::{
    offset::LocalResult, DateTime, Datelike, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone,
    Timelike,
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_postgres::{types::Type, NoTls};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Config {
    db_url: String,
    pico: String,
    pico_port: u16,
    station_id: i32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            db_url: "host = localhost user = humidity_temperature password = mypasswd dbname = humidity_temperature".to_string(),
            pico: "pico_host_here".to_string(),
            pico_port: 60438,
            station_id: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Measurement {
    time: DateTime<Local>,
    temp: i32,
    humidity: i32,
}

const CONFIG_PATH: &str = "config.json";

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    let config = match fs::read_to_string(CONFIG_PATH).await {
        Err(e) if e.kind() == ErrorKind::NotFound => {
            fs::write(
                CONFIG_PATH,
                serde_json::to_string_pretty(&Config::default())
                    .map_err(|err| anyhow!("Error serializing default config: {err}"))?,
            )
            .await?;
            eprintln!("no config file found; default config has been written to {CONFIG_PATH}. Please fill it out");
            return Ok(ExitCode::FAILURE);
        }
        result => result.map_err(|err| anyhow!("Error reading config file: {err}"))?,
    };

    let config: Config = serde_json::from_str(&config)
        .map_err(|err| anyhow!("Error deserializing config: {err}"))?;

    let (client, connection) = tokio_postgres::connect(&config.db_url, NoTls).await?;

    tokio::spawn(connection);

    let insert_statement = client
        .prepare_typed("insert into measurement(at, station_id, temp, humidity) values ($1, $2, $3::decimal / 10, $4::decimal / 10)", &[Type::TIMESTAMPTZ, Type::INT4, Type::INT4, Type::INT4])
        .await.map_err(|err| anyhow!("Error preparing measurement insertion statement: {err}"))?;

    let now = Local::now();

    let packed_now = [
        (now.second() as u8) & 0b111111 | (now.minute() as u8) << 6,
        (now.minute() as u8 >> 2) & 0b1111 | (now.hour() as u8) << 4,
        ((now.hour() as u8) >> 4) & 0b1
            | (now.weekday().number_from_sunday() as u8 - 1) << 1
            | (now.day0() as u8) << 4,
        ((now.day0() as u8) >> 4) & 0b1
            | (now.month0() as u8 & 0b1111) << 1
            | (now.year() as u8) << 5,
        ((now.year() as u16) >> 3) as u8,
        ((now.year() as u16) << 11) as u8,
    ];

    let mut pico_stream = TcpStream::connect((config.pico, config.pico_port))
        .await
        .map_err(|err| anyhow!("Error connecting to the Pico: {err}"))?;

    pico_stream
        .write_all(&packed_now)
        .await
        .map_err(|err| anyhow!("Error writing the packed date time to the Pico: {err}"))?;

    let measurement_count = pico_stream
        .read_u32_le()
        .await
        .map_err(|err| anyhow!("Error reading measurement count from Pico: {err}"))?;

    const SECTOR_COUNT: u32 = 512;
    const PAGES_PER_SECTOR: u32 = 16;
    const MEASUREMENTS_PER_PAGE: u32 = 32;

    if measurement_count > SECTOR_COUNT * PAGES_PER_SECTOR * MEASUREMENTS_PER_PAGE {
        eprintln!("Pico reported more than the theoretical maximum measurement count");
        return Ok(ExitCode::FAILURE);
    }

    let mut measurements = Vec::with_capacity(measurement_count as usize);

    loop {
        let packed_measurement = match pico_stream.read_u64_le().await {
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                break;
            }
            Err(err) => {
                return Err(anyhow!(
                    "Error reading a packed measurement from the Pico: {err}"
                ))
            }
            Ok(packed) => packed,
        };

        let datetime_result = Local.from_local_datetime(&NaiveDateTime::new(
            NaiveDate::from_ymd_opt(
                ((packed_measurement >> 26) & 0b1111_1111_1111_1111) as i32,
                (((packed_measurement >> 22) & 0b1111) + 1) as u32,
                (((packed_measurement >> 17) & 0b11111) + 1) as u32,
            )
            .ok_or(anyhow!("Pico sent invalid date"))?,
            NaiveTime::from_hms_opt(
                ((packed_measurement >> 12) & 0b11111) as u32,
                ((packed_measurement >> 6) & 0b111111) as u32,
                (packed_measurement & 0b111111) as u32,
            )
            .ok_or(anyhow!("Pico sent invalid time"))?,
        ));

        let time = match datetime_result {
            LocalResult::Single(datetime) => datetime,
            LocalResult::Ambiguous(_, _) => return Err(anyhow!("Pico sent ambiguous time")),
            LocalResult::None => return Err(anyhow!("Pico sent impossible time")),
        };

        measurements.push(Measurement {
            time,
            temp: ((packed_measurement >> 42) & 0b111111111) as i32,
            humidity: ((packed_measurement >> 51) & 0b1111111111) as i32,
        })
    }

    pico_stream
        .shutdown()
        .await
        .map_err(|err| anyhow!("Error shutting the connection to the Pico down: {err}"))?;

    drop(pico_stream);

    for measurement in measurements {
        client
            .execute(
                &insert_statement,
                &[
                    &measurement.time,
                    &config.station_id,
                    &measurement.temp,
                    &measurement.humidity,
                ],
            )
            .await
            .map_err(|err| anyhow!("Error inserting measurement: {err}"))?;
    }

    Ok(ExitCode::SUCCESS)
}
