"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const amqp = require("amqplib");
const email_config_1 = require("../config/email.config");
// Consume notification queue and send emails
const consumeNotificationQueue = () => __awaiter(void 0, void 0, void 0, function* () {
    const connection = yield amqp.connect("amqp://localhost");
    const channel = yield connection.createChannel();
    const queue = "emailQueue";
    // Assert the queue exists and is durable
    yield channel.assertQueue(queue, { durable: true });
    console.log("Waiting for messages in queue:", queue);
    // Handle incoming messages
    channel.consume(queue, (msg) => __awaiter(void 0, void 0, void 0, function* () {
        if (msg !== null) {
            try {
                const message = JSON.parse(msg.content.toString());
                const { email, subject, text } = message;
                console.log(message);
                const mailOptions = {
                    from: process.env.EMAIL_USER,
                    to: email,
                    subject,
                    text,
                };
                // Attempt to send the email
                yield email_config_1.transporter.sendMail(mailOptions);
                console.log("Email sent successfully:", email);
                // Acknowledge the message if email is sent
                channel.ack(msg);
            }
            catch (error) {
                console.error("Error sending email:", error);
                // Negative acknowledge the message to be requeued in case of error
                channel.nack(msg, false, true); // true will requeue the message
            }
        }
    }));
    // Graceful shutdown handler
    const gracefulShutdown = () => {
        console.log("Closing RabbitMQ connection gracefully...");
        channel.close();
        connection.close();
        process.exit(0);
    };
    // Listen for termination signals to shut down gracefully
    process.on("SIGINT", gracefulShutdown);
    process.on("SIGTERM", gracefulShutdown);
});
// Start consuming the queue
consumeNotificationQueue().catch((error) => {
    console.error("Error in consuming notifications:", error);
    process.exit(1);
});
