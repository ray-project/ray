// Verify the effective setting in the same JDK and configuration used by HS2.
// This small fixture probe does not introduce a Java testing framework.
public class ClockSkew {
  public static void main(String[] args) throws Exception {
    int seconds = sun.security.krb5.Config.getInstance().getIntValue("libdefaults", "clockskew");
    if (seconds != 1) {
      throw new IllegalStateException("Unexpected JVM Kerberos clockskew");
    }
    System.out.println("Effective JVM Kerberos clockskew=1s");
  }
}
