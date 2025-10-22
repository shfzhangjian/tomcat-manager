package com.zhangjian.tomcatmanager;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@SpringBootApplication
@EnableScheduling
public class TomcatManagerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TomcatManagerApplication.class, args);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}

// ============== CONFIGURATION ==============
@Configuration
class AppConfig {
    @Value("${tomcat.home.path}")
    private String tomcatHomePath;

    public String getTomcatHomePath() {
        return tomcatHomePath;
    }
}

// ============== DTO for Health Check Config ==============
class HealthCheckConfig {
    private boolean enabled = false;
    private String url = "";
    private int intervalSeconds = 30;
    private int failureThreshold = 3;

    // Getters and Setters
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public int getIntervalSeconds() { return intervalSeconds; }
    public void setIntervalSeconds(int intervalSeconds) { this.intervalSeconds = intervalSeconds; }
    public int getFailureThreshold() { return failureThreshold; }
    public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }
}


// ============== CONTROLLER ==============
@RestController
@RequestMapping("/api")
class TomcatController {
    private final TomcatService tomcatService;
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public TomcatController(TomcatService tomcatService) {
        this.tomcatService = tomcatService;
        this.tomcatService.setController(this);
    }

    @PostMapping("/start")
    public Map<String, String> start() {
        tomcatService.start();
        return Collections.singletonMap("message", "启动命令已发送。");
    }

    @PostMapping("/stop")
    public Map<String, String> stop() {
        tomcatService.stop();
        return Collections.singletonMap("message", "停止命令已发送。");
    }

    @PostMapping("/restart")
    public Map<String, String> restart() {
        tomcatService.restart();
        return Collections.singletonMap("message", "重启流程已启动。");
    }

    @PostMapping("/kill")
    public Map<String, String> kill() {
        tomcatService.killProcessByPort();
        return Collections.singletonMap("message", "Kill 命令已发送。");
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Collections.singletonMap("running", tomcatService.isTomcatRunning());
    }

    @GetMapping("/config")
    public Map<String, Object> getConfig() {
        return Collections.singletonMap("port", tomcatService.getTomcatPort());
    }

    @GetMapping("/webapps")
    public List<String> getWebapps() {
        return tomcatService.getDeployedWebapps();
    }

    @GetMapping("/subscribe")
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        // Send history first
        for (TomcatService.LogEntry entry : tomcatService.getLogBuffer()) {
            try {
                emitter.send(SseEmitter.event().name(entry.getType()).data(entry.getMessage()));
            } catch (IOException e) {
                // Ignore, client probably disconnected
            }
        }

        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));

        return emitter;
    }

    @PostMapping("/schedule")
    public Map<String, String> saveSchedule(@RequestBody Map<String, String> payload) {
        tomcatService.saveSchedule(payload.get("restartTime"));
        return Collections.singletonMap("message", "定时任务已保存。");
    }

    @GetMapping("/schedule")
    public Map<String, String> getSchedule() {
        return tomcatService.getSchedule();
    }

    @PostMapping("/health-check/config")
    public Map<String, String> updateHealthCheckConfig(@RequestBody HealthCheckConfig config) {
        tomcatService.updateHealthCheckConfig(config);
        return Collections.singletonMap("message", "健康检查配置已更新。");
    }

    @GetMapping("/health-check/config")
    public HealthCheckConfig getHealthCheckConfig() {
        return tomcatService.getHealthCheckConfig();
    }

    @PostMapping("/upload-webapp")
    public Map<String, String> uploadWebapp(@RequestParam("file") MultipartFile file) {
        String message = tomcatService.deployWebapp(file);
        return Collections.singletonMap("message", message);
    }

    @PostMapping("/uninstall-webapp")
    public Map<String, String> uninstallWebapp(@RequestBody Map<String, String> payload) {
        String appName = payload.get("appName");
        String password = payload.get("password");
        String message = tomcatService.undeployWebapp(appName, password);
        return Collections.singletonMap("message", message);
    }


    public void broadcast(String event, String data) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name(event).data(data));
            } catch (IOException e) {
                emitters.remove(emitter);
            }
        }
    }
}

// ============== SERVICE ==============
@Service
class TomcatService {
    private final AppConfig appConfig;
    private final ObjectMapper objectMapper;
    private TomcatController controller;
    private Process tomcatProcess;
    private Integer tomcatPort;
    private String scheduledRestartTime = null;
    private final Path scheduleFilePath = Paths.get("tomcat_schedule.json");
    private final Path healthCheckConfigPath = Paths.get("health_check_config.json");
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    private final List<LogEntry> logBuffer = new CopyOnWriteArrayList<>();
    private static final int MAX_LOG_BUFFER_SIZE = 200;

    private ScheduledExecutorService healthCheckExecutor;
    private HealthCheckConfig healthCheckConfig = new HealthCheckConfig();
    private int consecutiveFailures = 0;

    @Value("${tomcat.uninstall.password}")
    private String uninstallPassword;


    public TomcatService(AppConfig appConfig, ObjectMapper objectMapper) {
        this.appConfig = appConfig;
        this.objectMapper = objectMapper;
    }

    static class LogEntry {
        private final String message;
        private final String type;

        LogEntry(String message, String type) {
            this.message = message;
            this.type = type;
        }

        public String getMessage() { return message; }
        public String getType() { return type; }
    }

    public void setController(TomcatController controller) {
        this.controller = controller;
    }

    @PostConstruct
    public void init() {
        this.tomcatPort = parsePortFromConfig();
        if (this.tomcatPort == null) {
            this.tomcatPort = 8080; // Fallback
            broadcastLog("无法从 server.xml 自动读取端口，将使用默认端口 8080。", "error");
        }
        loadSchedule();
        loadHealthCheckConfig();
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (controller != null) {
                controller.broadcast("ping", "keep-alive");
            }
        }, 0, 15, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        heartbeatExecutor.shutdown();
        if (healthCheckExecutor != null) {
            healthCheckExecutor.shutdown();
        }
    }

    public void start() {
        if (isTomcatRunning()) {
            broadcastLog("Tomcat 已经在运行中。", "error");
            return;
        }
        try {
            String os = System.getProperty("os.name").toLowerCase();
            if (!os.contains("win")) {
                broadcastLog("此脚本仅支持 Windows。", "error");
                return;
            }

            File binDir = new File(appConfig.getTomcatHomePath(), "bin");
            ProcessBuilder pb = new ProcessBuilder(new File(binDir, "catalina.bat").getAbsolutePath(), "run");
            pb.directory(binDir);
            tomcatProcess = pb.start();

            redirectStream(tomcatProcess.getInputStream(), "log");
            redirectStream(tomcatProcess.getErrorStream(), "error");

            new Thread(() -> {
                try {
                    tomcatProcess.waitFor();
                    broadcastLog("[System] Tomcat 进程已停止。", "system");
                    broadcastStatusUpdate(false);
                    tomcatProcess = null;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    broadcastLog("[System] 等待 Tomcat 进程退出时被中断。", "error");
                }
            }).start();

            broadcastStatusUpdate(true);

        } catch (IOException e) {
            broadcastLog("启动 Tomcat 失败: " + e.getMessage(), "error");
        }
    }

    public void stop() {
        boolean wasRunning = isTomcatRunning();
        if (!wasRunning) {
            broadcastLog("[System] Tomcat 已经处于停止状态。", "system");
            broadcastStatusUpdate(false);
            return;
        }

        if (tomcatProcess != null && tomcatProcess.isAlive()) {
            try {
                broadcastLog("[System] 正在尝试优雅地停止 Tomcat...", "system");
                File binDir = new File(appConfig.getTomcatHomePath(), "bin");
                ProcessBuilder pb = new ProcessBuilder(new File(binDir, "catalina.bat").getAbsolutePath(), "stop");
                pb.start().waitFor();

                boolean stoppedGracefully = tomcatProcess.waitFor(30, TimeUnit.SECONDS);
                if (!stoppedGracefully) {
                    broadcastLog("[System] 优雅停止超时，将强制终止进程。", "error");
                }
            } catch (Exception e) {
                broadcastLog("执行优雅停止时出错: " + e.getMessage(), "error");
            } finally {
                tomcatProcess = null;
            }
        }

        if (isTomcatRunning()) {
            broadcastLog("[System] 进程仍在运行，执行强制 Kill。", "system");
            killProcessByPort();
        } else {
            scheduleStatusUpdate();
        }
    }

    public void restart() {
        broadcastLog("[System] 开始重启 Tomcat...", "system");
        if (isTomcatRunning()) {
            stop();
            new Thread(() -> {
                try {
                    for (int i = 0; i < 15; i++) { // Max wait 15 seconds
                        if (!isTomcatRunning()) {
                            break;
                        }
                        Thread.sleep(1000);
                    }
                    if (isTomcatRunning()) {
                        broadcastLog("[System] 重启失败：无法停止旧的 Tomcat 进程。", "error");
                    } else {
                        broadcastLog("[System] 旧进程已停止，准备启动新进程...", "system");
                        start();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        } else {
            start();
        }
    }

    private void redirectStream(InputStream inputStream, String type) {
        new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("GBK")))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    broadcastLog(line, type);
                }
            } catch (IOException e) {
                // Stream closed
            }
        }).start();
    }

    private String getPidForPort() {
        try {
            String os = System.getProperty("os.name").toLowerCase();
            if (!os.contains("win")) {
                return null;
            }
            String command = "netstat -ano | findstr \":" + tomcatPort + "\"";
            Process p = new ProcessBuilder("cmd.exe", "/c", command).start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String listeningLine = reader.lines()
                        .filter(line -> line.trim().contains("LISTENING"))
                        .findFirst()
                        .orElse(null);

                if (listeningLine != null) {
                    String[] parts = listeningLine.trim().split("\\s+");
                    if (parts.length > 0) {
                        return parts[parts.length - 1];
                    }
                }
            }
            p.waitFor();
        } catch (Exception e) {
            broadcastLog("[System] 检查端口进程时出错: " + e.getMessage(), "error");
        }
        return null;
    }

    private boolean isJavaProcess(String pid) {
        if (pid == null || pid.isEmpty()) {
            return false;
        }
        try {
            String command = "tasklist /fi \"PID eq " + pid + "\"";
            Process p = new ProcessBuilder("cmd.exe", "/c", command).start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                return reader.lines().anyMatch(line -> {
                    String lowerLine = line.toLowerCase();
                    return lowerLine.startsWith("java.exe") || lowerLine.startsWith("javaw.exe");
                });
            }
        } catch (Exception e) {
            broadcastLog("[System] 检查Java进程时出错: " + e.getMessage(), "error");
        }
        return false;
    }

    public boolean isTomcatRunning() {
        if (tomcatProcess != null && tomcatProcess.isAlive()) {
            return true;
        }
        String pidListeningOnPort = getPidForPort();
        return pidListeningOnPort != null && isJavaProcess(pidListeningOnPort);
    }

    public void killProcessByPort() {
        try {
            String pid = getPidForPort();
            if (pid != null && !pid.isEmpty()) {
                new ProcessBuilder("taskkill", "/F", "/PID", pid).start().waitFor();
                broadcastLog("[System] 已发送 Kill 命令到 PID: " + pid, "system");
            } else {
                broadcastLog("[System] 未找到监听端口 " + tomcatPort + " 的进程。", "system");
            }
        } catch (Exception e) {
            broadcastLog("Kill 进程失败: " + e.getMessage(), "error");
        }
        scheduleStatusUpdate();
    }

    private Integer parsePortFromConfig() {
        File serverXml = new File(appConfig.getTomcatHomePath(), "conf/server.xml");
        if (!serverXml.exists()) {
            broadcastLog("错误: 在 " + serverXml.getAbsolutePath() + " 未找到 server.xml 文件。", "error");
            return null;
        }
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            dbFactory.setXIncludeAware(false);
            dbFactory.setExpandEntityReferences(false);

            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(serverXml);
            doc.getDocumentElement().normalize();
            NodeList connectors = doc.getElementsByTagName("Connector");
            for (int i = 0; i < connectors.getLength(); i++) {
                Element connector = (Element) connectors.item(i);

                if (connector.hasAttribute("port")) {
                    String protocol = connector.getAttribute("protocol");
                    if (protocol != null && protocol.toUpperCase().contains("AJP")) {
                        continue;
                    }
                    return Integer.parseInt(connector.getAttribute("port"));
                }
            }
        } catch (Exception e) {
            broadcastLog("解析 server.xml 失败: " + e.getMessage(), "error");
            return null;
        }
        broadcastLog("在 server.xml 中未找到有效的 HTTP Connector 端口。", "error");
        return null;
    }

    public Integer getTomcatPort(){
        return this.tomcatPort;
    }

    public List<String> getDeployedWebapps() {
        File webappsDir = new File(appConfig.getTomcatHomePath(), "webapps");
        if (webappsDir.exists() && webappsDir.isDirectory()) {
            File[] files = webappsDir.listFiles();
            if (files == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(files)
                    .filter(file -> file.isDirectory() && !file.getName().equals("ROOT"))
                    .map(File::getName)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public void saveSchedule(String time) {
        this.scheduledRestartTime = time;
        try {
            Map<String, String> schedule = Collections.singletonMap("restartTime", time);
            Files.write(scheduleFilePath, objectMapper.writeValueAsBytes(schedule));
        } catch (IOException e) {
            broadcastLog("保存定时任务失败: " + e.getMessage(), "error");
        }
    }

    @SuppressWarnings("unchecked")
    private void loadSchedule() {
        if (Files.exists(scheduleFilePath)) {
            try {
                byte[] bytes = Files.readAllBytes(scheduleFilePath);
                Map<String, String> schedule = objectMapper.readValue(bytes, Map.class);
                this.scheduledRestartTime = schedule.get("restartTime");
            } catch (IOException e) {
                broadcastLog("加载定时任务失败: " + e.getMessage(), "error");
            }
        }
    }

    public Map<String, String> getSchedule() {
        return Collections.singletonMap("restartTime", this.scheduledRestartTime);
    }

    @Scheduled(cron = "0 * * * * ?")
    public void checkScheduledRestart() {
        if (scheduledRestartTime == null || scheduledRestartTime.isEmpty()) {
            return;
        }
        String now = new java.text.SimpleDateFormat("HH:mm").format(new Date());
        if (now.equals(scheduledRestartTime) && isTomcatRunning()) {
            broadcastLog("[System] 执行定时重启任务...", "system");
            restart();
        }
    }

    public HealthCheckConfig getHealthCheckConfig() {
        return this.healthCheckConfig;
    }

    public void updateHealthCheckConfig(HealthCheckConfig newConfig) {
        this.healthCheckConfig = newConfig;
        saveHealthCheckConfig();

        if (healthCheckExecutor != null) {
            healthCheckExecutor.shutdownNow();
        }

        if (healthCheckConfig.isEnabled()) {
            startHealthChecker();
        }
    }

    private void saveHealthCheckConfig() {
        try {
            Files.write(healthCheckConfigPath, objectMapper.writeValueAsBytes(healthCheckConfig));
        } catch (IOException e) {
            broadcastLog("保存健康检查配置失败: " + e.getMessage(), "error");
        }
    }

    private void loadHealthCheckConfig() {
        if (Files.exists(healthCheckConfigPath)) {
            try {
                this.healthCheckConfig = objectMapper.readValue(healthCheckConfigPath.toFile(), HealthCheckConfig.class);
                if (this.healthCheckConfig.isEnabled()) {
                    startHealthChecker();
                }
            } catch (IOException e) {
                broadcastLog("加载健康检查配置失败: " + e.getMessage(), "error");
                this.healthCheckConfig = new HealthCheckConfig();
            }
        }
    }

    private void startHealthChecker() {
        if (healthCheckConfig.getIntervalSeconds() <= 0) return;
        healthCheckExecutor = Executors.newSingleThreadScheduledExecutor();
        healthCheckExecutor.scheduleAtFixedRate(this::performHealthCheck, 10, healthCheckConfig.getIntervalSeconds(), TimeUnit.SECONDS);
        broadcastLog("[System] 健康检查任务已启动。频率: " + healthCheckConfig.getIntervalSeconds() + "秒/次。", "system");
    }

    private void performHealthCheck() {
        if (!healthCheckConfig.isEnabled() || !isTomcatRunning()) {
            consecutiveFailures = 0;
            return;
        }

        HttpURLConnection connection = null;
        try {
            URL url = new URL(healthCheckConfig.getUrl());
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);

            int responseCode = connection.getResponseCode();
            if (responseCode >= 200 && responseCode < 400) {
                if (consecutiveFailures > 0) {
                    broadcastLog("[System] 健康检查恢复正常。", "system");
                }
                consecutiveFailures = 0;
            } else {
                handleHealthCheckFailure("服务器返回错误码: " + responseCode);
            }
        } catch (IOException e) {
            handleHealthCheckFailure("连接失败: " + e.getMessage());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private void handleHealthCheckFailure(String reason) {
        consecutiveFailures++;
        int threshold = healthCheckConfig.getFailureThreshold();
        broadcastLog(String.format("[System] 健康检查失败 (%d/%d): %s", consecutiveFailures, threshold, reason), "error");
        if (consecutiveFailures >= threshold) {
            broadcastLog(String.format("[System] 健康检查连续失败 %d 次，触发自动重启...", threshold), "system");
            restart();
            consecutiveFailures = 0;
        }
    }

    public List<LogEntry> getLogBuffer() {
        return logBuffer;
    }

    public String deployWebapp(MultipartFile file) {
        if (file.isEmpty()) {
            return "部署失败：上传的文件为空。";
        }

        String fileName = file.getOriginalFilename();
        if (fileName == null || !fileName.toLowerCase().endsWith(".war")) {
            return "部署失败：请上传一个 .war 文件。";
        }

        Path webappsDir = Paths.get(appConfig.getTomcatHomePath(), "webapps");
        if (!Files.isDirectory(webappsDir)) {
            broadcastLog("错误: Tomcat webapps 目录不存在: " + webappsDir, "error");
            return "部署失败：找不到 Tomcat webapps 目录。";
        }

        try {
            Path destinationFile = webappsDir.resolve(fileName);
            file.transferTo(destinationFile.toFile());
            broadcastLog("[System] 文件 " + fileName + " 已成功上传到 webapps 目录。", "system");

            scheduleStatusUpdate();

            return "文件 " + fileName + " 部署成功。Tomcat 将会自动解压并加载应用。";
        } catch (IOException e) {
            broadcastLog("部署 " + fileName + " 失败: " + e.getMessage(), "error");
            return "部署失败：" + e.getMessage();
        }
    }

    public String undeployWebapp(String appName, String password) {
        if (appName == null || appName.isEmpty()) {
            return "卸载失败：应用名称不能为空。";
        }

        if (!uninstallPassword.equals(password)) {
            broadcastLog("[System] 尝试卸载 " + appName + " 失败：密码错误。", "error");
            return "卸载失败：密码错误。";
        }

        Path webappsDir = Paths.get(appConfig.getTomcatHomePath(), "webapps");
        Path appDir = webappsDir.resolve(appName);
        Path warFile = webappsDir.resolve(appName + ".war");

        boolean deletedSomething = false;

        try {
            if (Files.isDirectory(appDir)) {
                broadcastLog("[System] 正在删除目录: " + appDir, "system");
                deleteDirectory(appDir.toFile());
                deletedSomething = true;
            }
            if (Files.exists(warFile)) {
                broadcastLog("[System] 正在删除文件: " + warFile, "system");
                Files.delete(warFile);
                deletedSomething = true;
            }

            if (deletedSomething) {
                String successMsg = "应用 " + appName + " 已成功卸载。";
                broadcastLog("[System] " + successMsg, "system");
                scheduleStatusUpdate();
                return successMsg;
            } else {
                return "卸载失败：未找到与 " + appName + " 相关的文件或目录。";
            }
        } catch (IOException e) {
            String errorMsg = "卸载 " + appName + " 过程中发生错误: " + e.getMessage();
            broadcastLog(errorMsg, "error");
            return errorMsg;
        }
    }

    private void deleteDirectory(File directory) {
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }

    private void broadcastLog(String message, String type) {
        LogEntry entry = new LogEntry(message, type);
        logBuffer.add(entry);
        if (logBuffer.size() > MAX_LOG_BUFFER_SIZE) {
            logBuffer.remove(0);
        }

        if (controller != null) {
            controller.broadcast(type, message);
        }
        System.out.println("[" + type.toUpperCase() + "] " + message);
    }

    private void broadcastStatusUpdate(boolean running) {
        if (controller != null) {
            controller.broadcast("status", "{\"running\":" + running + "}");
        }
    }

    private void scheduleStatusUpdate() {
        new Thread(() -> {
            try {
                Thread.sleep(5000);
                broadcastStatusUpdate(isTomcatRunning());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
}

