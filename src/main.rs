use prometheus::{Encoder, Registry, TextEncoder};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::{info, warn, error};
use tokio::time::{sleep, Duration};

use crate::{error::Error, metrics::Metrics};
use starlink::proto::space_x::api::device::{
    device_client::DeviceClient,
    request,
    response,
    GetDeviceInfoRequest,
    Request,
};

mod error;
mod metrics;

use std::fs::OpenOptions;
use std::io::Write;
use chrono::Local;
use std::process::Command;

// Проверка подключения к Starlink WiFi
fn is_connected_to_starlink_wifi() -> bool {
    if let Ok(output) = Command::new("termux-wifi-connectioninfo")
        .output()
    {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.contains("STARLINK") || stdout.contains("Starlink") || stdout.contains("starlink") {
            info!("Currently connected to Starlink WiFi");
            return true;
        } else {
            info!("Connected to WiFi but NOT Starlink");
        }
    }
    false
}

// Получить список доступных WiFi сетей
fn get_available_wifi_networks() -> Option<String> {
    if let Ok(output) = Command::new("termux-wifi-scaninfo")
        .output()
    {
        Some(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        None
    }
}

// Найти Starlink сеть в списке доступных
fn find_starlink_network(networks: &str) -> Option<String> {
    for line in networks.lines() {
        let line_upper = line.to_uppercase();
        if line_upper.contains("STARLINK") {
            // Извлекаем SSID (обычно в формате "Starlink" или "STARLINK-XXXX")
            if let Some(start) = line.find("\"") {
                if let Some(end) = line[start+1..].find("\"") {
                    let ssid = line[start+1..start+1+end].to_string();
                    info!("Found Starlink network with SSID: {}", ssid);
                    return Some(ssid);
                }
            }
            // Если не нашли в кавычках, ищем по ключевым словам
            info!("Found Starlink network (no quotes)");
            return Some("Starlink".to_string());
        }
    }
    None
}

// Переподключиться к Starlink WiFi - теперь ищет бесконечно
async fn reconnect_to_starlink() -> bool {
    info!("=== Starting Starlink WiFi reconnection process ===");
    
    loop {
        // Шаг 1: Проверяем, может мы уже подключены к Starlink?
        if is_connected_to_starlink_wifi() {
            info!("Already connected to Starlink WiFi!");
            return true;
        }
        
        // Шаг 2: Выключаем WiFi для сброса соединения
        info!("Disabling WiFi to reset connection...");
        if let Err(e) = Command::new("termux-wifi-enable")
            .arg("false")
            .output()
        {
            error!("Failed to disable WiFi: {}", e);
        }
        sleep(Duration::from_secs(3)).await;
        
        // Шаг 3: Включаем WiFi
        info!("Enabling WiFi...");
        if let Err(e) = Command::new("termux-wifi-enable")
            .arg("true")
            .output()
        {
            error!("Failed to enable WiFi: {}", e);
            sleep(Duration::from_secs(5)).await;
            continue;
        }
        sleep(Duration::from_secs(5)).await;
        
        // Шаг 4: Ищем Starlink сеть
        info!("Scanning for Starlink network...");
        
        // Цикл поиска Starlink сети - пока не найдём
        let starlink_ssid = loop {
            sleep(Duration::from_secs(3)).await;
            
            if let Some(networks) = get_available_wifi_networks() {
                info!("Scanning available networks...");
                if let Some(ssid) = find_starlink_network(&networks) {
                    info!("? Found Starlink network: {}", ssid);
                    break ssid;
                } else {
                    warn!("Starlink network not found in scan results. Will scan again in 5 seconds...");
                }
            } else {
                error!("Failed to scan WiFi networks. Retrying in 5 seconds...");
            }
        };
        
        // Шаг 5: Подключаемся к найденной сети Starlink
        info!("Attempting to connect to Starlink network: {}", starlink_ssid);
        
        // Пробуем подключиться через сохраненные сети Android
        if let Err(e) = Command::new("termux-wifi-enable")
            .arg("true")
            .output()
        {
            error!("Failed to trigger WiFi connection: {}", e);
        }
        
        // Ждем подключения бесконечно
        info!("Waiting for connection to establish...");
        loop {
            sleep(Duration::from_secs(2)).await;
            
            if is_connected_to_starlink_wifi() {
                info!("? Successfully connected to Starlink WiFi!");
                return true;
            }
            
            // Проверяем, не потерялась ли сеть
            if let Some(networks) = get_available_wifi_networks() {
                if !networks.to_uppercase().contains("STARLINK") {
                    warn!("Starlink network disappeared! Starting search again...");
                    break; // Выходим во внешний цикл для нового поиска
                }
            }
            
            info!("Still waiting for WiFi connection to Starlink...");
        }
    }
}

// Мониторинг WiFi соединения в фоне
async fn wifi_monitor_task(starlink_address: String) {
    let wifi_check_interval = Duration::from_secs(10);
    
    loop {
        if !is_connected_to_starlink_wifi() {
            warn!("? WiFi connection to Starlink LOST! Starting automatic reconnection...");
            warn!("Current connection is NOT Starlink. Will search for Starlink network...");
            
            reconnect_to_starlink().await;
            
            info!("WiFi reconnection process completed. Waiting for network to stabilize...");
            sleep(Duration::from_secs(10)).await;
        } else {
            info!("? WiFi connection to Starlink is active");
        }
        
        sleep(wifi_check_interval).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let starlink_address = dotenv::var("STARLINK_ADDRESS").unwrap_or("http://dishy.starlink.com:9200".to_string());

    info!("=== Starlink Monitor with Intelligent WiFi Management ===");
    info!("Target device: {}", &starlink_address);
    info!("Initial WiFi check...");
    
    // Проверяем и подключаемся к Starlink WiFi при запуске
    if !is_connected_to_starlink_wifi() {
        warn!("Not connected to Starlink WiFi. Starting search and connection process...");
        reconnect_to_starlink().await;
    }

    // Запускаем фоновый мониторинг WiFi
    let wifi_address = starlink_address.clone();
    tokio::spawn(async move {
        wifi_monitor_task(wifi_address).await;
    });

    info!("Connecting to Starlink device on {}", &starlink_address);

    let mut labels = HashMap::new();
    let mut connected = false;

    // Initial connection loop
    while !connected {
        match DeviceClient::connect(starlink_address.clone()).await {
            Ok(mut client) => {
                let req = tonic::Request::new(Request {
                    request: Some(request::Request::GetDeviceInfo(GetDeviceInfoRequest {})),
                    ..Default::default()
                });
                
                match client.handle(req).await {
                    Ok(res) => {
                        let res = res.into_inner();
                        if let Some(response::Response::GetDeviceInfo(r)) = res.response {
                            if let Some(device_info) = r.device_info {
                                if let Some(id) = device_info.id {
                                    info!("Device ID: {}", &id);
                                    labels.insert("id".to_string(), id);
                                }
                                if let Some(hardware_version) = device_info.hardware_version {
                                    info!("Hardware version: {}", &hardware_version);
                                    labels.insert("hardware_version".to_string(), hardware_version);
                                }
                            }
                        }
                        connected = true;
                        info!("? Successfully connected to Starlink device");
                    }
                    Err(e) => {
                        warn!("Failed to get device info: {}. Retrying in 5 seconds...", e);
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
            Err(e) => {
                warn!("Connection failed: {}. Will check WiFi and retry...", e);
                
                // Проверяем WiFi при проблемах с подключением
                if !is_connected_to_starlink_wifi() {
                    warn!("WiFi also appears to be disconnected. Starting WiFi recovery...");
                    reconnect_to_starlink().await;
                }
                
                sleep(Duration::from_secs(5)).await;
            }
        }
    }

    let registry = Registry::new_custom(Some("starlink".to_string()), Some(labels))?;

    let metrics = Metrics::new()?;
    metrics.register(&registry)?;
    let metrics = Arc::new(Mutex::new(metrics));

    info!("Starting continuous monitoring and logging to starlink_metrics_detailed.log");
    info!("Program will maintain Starlink WiFi connection and log all metrics");

    // Main loop with intelligent reconnection
    loop {
        let mut metrics_lock = metrics.lock().await;
        
        match metrics_lock.update(starlink_address.clone()).await {
            Ok(_) => {
                // Успешно получили метрики - логируем
                let encoder = TextEncoder::new();
                let metric_families = registry.gather();
                let mut buffer = vec![];
                
                if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
                    error!("Encode error: {}", e);
                    // Продолжаем работу даже при ошибке кодирования
                } else {
                    let metrics_str = String::from_utf8_lossy(&buffer);
                    if let Err(e) = log_metrics_to_file(&metrics_str) {
                        error!("Log write error: {}", e);
                        // Продолжаем работу даже при ошибке записи
                    } else {
                        info!("? Metrics logged at {}", Local::now().format("%H:%M:%S"));
                    }
                }
            }
            Err(e) => {
                // Связь с устройством потеряна, НО продолжаем писать логи
                error!("? Connection to Starlink device LOST: {}", e);
                warn!("Writing error to log and attempting recovery...");
                
                // Логируем ошибку в файл
                let error_msg = format!("# Connection lost at {}: {}\n", 
                    Local::now().format("%Y-%m-%d %H:%M:%S"), e);
                if let Err(write_err) = log_metrics_to_file(&error_msg) {
                    error!("Failed to write error to log: {}", write_err);
                }
                
                // Проверяем WiFi соединение
                if !is_connected_to_starlink_wifi() {
                    warn!("WiFi connection to Starlink also lost. Initiating WiFi recovery...");
                    drop(metrics_lock);
                    reconnect_to_starlink().await;
                    metrics_lock = metrics.lock().await;
                }
                
                // Пытаемся переподключиться к устройству
                drop(metrics_lock);
                
                info!("Attempting to reconnect to Starlink device...");
                loop {
                    // Проверяем WiFi перед каждой попыткой
                    if !is_connected_to_starlink_wifi() {
                        warn!("WiFi lost during device reconnection. Recovering WiFi first...");
                        reconnect_to_starlink().await;
                    }
                    
                    match DeviceClient::connect(starlink_address.clone()).await {
                        Ok(mut client) => {
                            let req = tonic::Request::new(Request {
                                request: Some(request::Request::GetDeviceInfo(GetDeviceInfoRequest {})),
                                ..Default::default()
                            });
                            
                            match client.handle(req).await {
                                Ok(_) => {
                                    info!("? Successfully reconnected to Starlink device! Resuming telemetry...");
                                    // Логируем восстановление связи
                                    let recovery_msg = format!("# Connection restored at {}\n", 
                                        Local::now().format("%Y-%m-%d %H:%M:%S"));
                                    let _ = log_metrics_to_file(&recovery_msg);
                                    break;
                                }
                                Err(e) => {
                                    warn!("Device reconnection attempt failed: {}. Retrying in 5 seconds...", e);
                                    sleep(Duration::from_secs(5)).await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Device connection failed: {}. Retrying in 5 seconds...", e);
                            sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
                continue;
            }
        }

        drop(metrics_lock);
        sleep(Duration::from_secs(5)).await;
    }
}

fn log_metrics_to_file(metrics_text: &str) -> std::io::Result<()> {
    let filename = "starlink_metrics_detailed.log";
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)?;

    writeln!(file, "=")?;
    writeln!(file, "[{}] --- LOG ENTRY ---", timestamp)?;
    writeln!(file, "{}", metrics_text)?;
    writeln!(file, "=")?;
    writeln!(file)?;
    
    file.sync_all()?;
    
    Ok(())
}