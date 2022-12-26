package io.ray.serve.util;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("example")
public class ExampleJaxrsDeployment {
  @GET
  @Path("helloWorld")
  @Consumes({"application/json"})
  @Produces({"application/json"})
  public String helloWorld(@QueryParam("name") String name) {
    return "hello world, " + name;
  }

  @GET
  @Path("paramPathTest/{name}")
  @Consumes({"application/json"})
  @Produces({"application/json"})
  public String paramPathTest(@PathParam("name") String name) {
    return "paramPathTest, " + name;
  }

  @POST
  @Path("helloWorld2")
  @Consumes({"application/json"})
  @Produces({"application/json"})
  public String helloWorld2(String name) {
    return "hello world, i am from body:" + name;
  }
}
