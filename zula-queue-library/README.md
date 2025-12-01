# Zula Queue Library

Automated RabbitMQ queue management for Zula microservices.

This library provides a base consumer and publisher to simplify queue creation and message handling across Zula services. The base consumer will programmatically register a listener container when a RabbitMQ ConnectionFactory is available, so consuming applications do not need to use `@RabbitListener` with SpEL-based queue expressions.

Requirements
- Java 17 (compiled and published with release 17)
- RabbitMQ available for runtime if you want consumers to start automatically

Usage

Add dependency:

```xml
<dependency>
    <groupId>com.zula</groupId>
    <artifactId>zula-queue-library</artifactId>
    <version>1.0.7</version>
</dependency>
```

Consumer notes
- Extend `com.zula.queue.core.BaseMessageConsumer<T>` and implement `processMessage(T message)`.
- Do NOT rely on SpEL inside `@RabbitListener` annotations to reference `messageType`. The base consumer will register a listener automatically if a `ConnectionFactory` bean is present. Example consumer:

```java
@Service
public class AuthResponseConsumer extends BaseMessageConsumer<AuthResponseMessage> {
    public AuthResponseConsumer() {
        super("auth-response"); // optional; otherwise derived from generic
    }

    @Override
    public void processMessage(AuthResponseMessage message) {
        // handle message
    }
}
```

Publishing to GitHub Packages (Maven)

1. Ensure your `pom.xml` has the `distributionManagement` repository pointing to GitHub Packages (the `pom.xml` in this repo is already configured for `https://maven.pkg.github.com/Bico-Steve101/zula`).

2. Configure your Maven `settings.xml` with a GitHub Packages token (create a personal access token with `read:packages, write:packages` scopes):

```xml
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>GITHUB_USERNAME</username>
      <password>GITHUB_PERSONAL_ACCESS_TOKEN</password>
    </server>
  </servers>
</settings>
```

3. Build and deploy with:

```bash
mvn -DskipTests=true clean deploy
```

Notes
- The library is compiled with Java 17. If your consuming service runs on an older JDK, either upgrade it to Java 17 or change the library's compiler target to a lower Java version.
- If your application uses custom listener container configuration or wants to manage `@RabbitListener` usage directly, you can disable the programmatic listener by removing the `ConnectionFactory` bean injection or by choosing not to rely on the base consumer's auto-start behavior.

License
- MIT (choose your license)
