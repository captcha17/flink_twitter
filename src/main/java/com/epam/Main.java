package com.epam;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.LinkedHashMap;
import java.util.Properties;

/**
 * Created by Dzmitry_Rakushau on 6/19/2018.
 */
public class Main {
    public static final String DESIRED_HASHTAG = "";
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(100);

        TwitterSource twitterSource = new TwitterSource(Util.getTwitterProps());
        DataStream<String> streamSource = env.addSource(twitterSource);

        SingleOutputStreamOperator<Tuple2<String, String>> tweets = streamSource
                .flatMap(new TweetFlatMapper());
//                .filter((FilterFunction<Tuple2<String, String>>) value -> value.getField(0) != null && ((String)value.getField(0)).contains(DESIRED_HASHTAG));

        DataStream<LinkedHashMap<String, Integer>> ds = tweets.timeWindowAll(Time.seconds(300), Time.seconds(5))
                .apply(new MostPopularTags());

        ds.print();
//        tweets.addSink((SinkFunction<Tuple2<String, String>>) Main::sendEmail);

        // execute program
        env.execute("Twitter Streaming Example");
    }
    
    public static void sendEmail(Tuple2<String, String> tuple) throws MessagingException {
        String text = String.format("Found tweed with hashtag %s. Link: %s", tuple.getField(0), tuple.getField(1));
        Session session = createMailSession();
        Message message = new MimeMessage(session);
        message.addRecipients(Message.RecipientType.TO,
                InternetAddress.parse("dzmitry_rakushau@epam.com"));
        message.setSubject("test subject");
        MimeMultipart msgBody = new MimeMultipart("mixed");
        MimeBodyPart htmlPart = new MimeBodyPart();
        htmlPart.setHeader("Content-Transfer-Encoding", "base64");
        htmlPart.setContent(text, "text/html; charset=UTF-8");
        msgBody.addBodyPart(htmlPart);
        message.setContent(msgBody);
        Transport.send(message);
    }

    protected static Session createMailSession() {
        Properties props = new Properties();
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.socketFactory.port", "465");
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.auth", true);
        props.put("mail.smtp.port", 465);

        return Session.getInstance(
                props,
                new javax.mail.Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication("userName", "password");
                    }
                });
    }
}
