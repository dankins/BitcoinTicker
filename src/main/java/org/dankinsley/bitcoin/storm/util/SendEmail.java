package org.dankinsley.bitcoin.storm.util;

import java.io.IOException;
import com.amazonaws.services.simpleemail.*;
import com.amazonaws.services.simpleemail.model.*;
import com.amazonaws.auth.PropertiesCredentials;

public class SendEmail {
 
    static final String FROM = "SENDER@EXAMPLE.COM";  // Replace with your "From" address. This address must be verified.
    static final String TO = "RECIPIENT@EXAMPLE.COM"; // Replace with a "To" address. If you have not yet requested
                                                      // production access, this address must be verified.
    static final String BODY = "Bitcoin Price Alert";
  

    public static void sendAlert(String subjectLine) throws IOException {
    	
        // Your AWS credentials are stored in the AwsCredentials.properties file within the project.
        // You entered these AWS credentials when you created a new AWS Java project in Eclipse.
        PropertiesCredentials credentials = new PropertiesCredentials(
        		SendEmail.class
                        .getResourceAsStream("AwsCredentials.properties"));
        
        // Retrieve the AWS Access Key ID and Secret Key from AwsCredentials.properties.
        credentials.getAWSAccessKeyId();
        credentials.getAWSSecretKey();
    
        // Construct an object to contain the recipient address.
        Destination destination = new Destination().withToAddresses(new String[]{TO});
        
        // Create the subject and body of the message.
        Content subject = new Content().withData(subjectLine);
        Content textBody = new Content().withData(BODY); 
        Body body = new Body().withText(textBody);
        
        // Create a message with the specified subject and body.
        Message message = new Message().withSubject(subject).withBody(body);
        
        // Assemble the email.
        SendEmailRequest request = new SendEmailRequest().withSource(FROM).withDestination(destination).withMessage(message);
        
        try
        {        
            System.out.println("Attempting to send an email through Amazon SES by using the AWS SDK for Java...");
        
            // Instantiate an Amazon SES client, which will make the service call with the supplied AWS credentials.
            AmazonSimpleEmailServiceClient client = new AmazonSimpleEmailServiceClient(credentials);
       
            // Send the email.
            client.sendEmail(request);  
            System.out.println("Email sent!");
        }
        catch (Exception ex) 
        {
            System.out.println("The email was not sent.");
            System.out.println("Error message: " + ex.getMessage());
        }
    }
}