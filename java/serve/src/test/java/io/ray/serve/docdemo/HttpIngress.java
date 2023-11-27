package io.ray.serve.docdemo;

// api-http-start
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import java.io.IOException;
import org.apache.hc.client5.http.fluent.Request;

public class HttpIngress {

	public String call(String request) {
    return "Hello " + request + "!";
	}

  public static void main(String[] args) throws IOException {
    Application app = Serve.deployment().setDeploymentDef(HttpIngress.class.getName()).bind();
    Serve.run(app);

    System.out.println(
        Request.post("http://127.0.0.1:8000/")
            .bodyString("Corey", null)
            .execute()
            .returnContent()
            .asString());
  }
}
// api-http-end
