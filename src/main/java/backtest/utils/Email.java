package backtest.utils;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.MultiPartEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * To send email
 * Email email = Email.getInstance();
 * //email.reset(); //clear all previous content if you want
 * email.println("....."); //append a line in the email
 * email.send(); //send the email
 * <p>
 * To modify recipients, look at last few lines of send method
 * use addTo method to add recipients.
 */
public class Email {
  private static Logger logger = LoggerFactory.getLogger(Email.class);
  private String msg = "";
  private boolean printToConsole = false;
  private List<String> recipients;

  public Email(){
    recipients = new ArrayList<>();
  }

  public void addRecipient(String emailAddr) {
    recipients.add(emailAddr);
  }

  public void reset() {
    msg = "";
  }

  public void alsoPrintToConsole() {printToConsole = true;}

  private void append(String str) {
    msg += str;
  }

  public synchronized void println(String str) {
    append(str + "\n");
    if (printToConsole) logger.info(str);
  }

  public void send() {
    if (msg.equals("")) return;
    logger.info("send email");
    MultiPartEmail email = new MultiPartEmail();
    email.setHostName("smtp.googlemail.com");
    email.setSmtpPort(465);
    email.setAuthenticator(new DefaultAuthenticator("please.dont.reply.to.here", "nevorbalyjptsmqy"));
    email.setSSLOnConnect(true);
    try {
      LocalDate now = LocalDate.now();
      email.setFrom("please.dont.reply.to.here@gmail.com");
      email.setSubject("report " + now);
      email.setMsg(msg);
      for (String recipient : recipients) {
        email.addTo(recipient);
      }
      email.send();
      reset();
    } catch (EmailException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Email email = new Email();
    email.println("test");
    email.send();
  }
}
