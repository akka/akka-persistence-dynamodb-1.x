package akka.persistence.dynamodb

import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.Container
import org.scalatest.Suite
import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy

/**
 * Base spec for tests that verify integration with DynamoDB.
 */
trait IntegSpec extends ForAllTestContainer { self: Suite =>
  // TODO: Use dynamic ports. This is a annoying to do as the actor system is init prior to beforeAll.
  override val container: Container = FixedHostPortGenericContainer(
    "amazon/dynamodb-local:latest",
    exposedContainerPort = 8000,
    exposedHostPort = 8888,
    waitStrategy = new HttpWaitStrategy().forPath("/").forStatusCode(400))
}
