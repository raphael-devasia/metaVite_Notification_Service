const amqp = require("amqplib")
import { transporter } from "../config/email.config"

// Consume notification queue and send emails
const consumeNotificationQueue = async () => {
    const connection = await amqp.connect("amqp://localhost")
    const channel = await connection.createChannel()
    const queue = "emailQueue"

    // Assert the queue exists and is durable
    await channel.assertQueue(queue, { durable: true })
    console.log("Waiting for messages in queue:", queue)

    // Handle incoming messages
    channel.consume(queue, async (msg:any) => {
        if (msg !== null) {
            try {
                const message = JSON.parse(msg.content.toString())
                const { email, subject, text } = message
                console.log(message)
                
                const mailOptions = {
                    from: process.env.EMAIL_USER,
                    to: email,
                    subject,
                    text,
                }

                // Attempt to send the email
                await transporter.sendMail(mailOptions)
                console.log("Email sent successfully:", email)

                // Acknowledge the message if email is sent
                channel.ack(msg)
            } catch (error) {
                console.error("Error sending email:", error)

                // Negative acknowledge the message to be requeued in case of error
                channel.nack(msg, false, true) // true will requeue the message
            }
        }
    })

    // Graceful shutdown handler
    const gracefulShutdown = () => {
        console.log("Closing RabbitMQ connection gracefully...")
        channel.close()
        connection.close()
        process.exit(0)
    }

    // Listen for termination signals to shut down gracefully
    process.on("SIGINT", gracefulShutdown)
    process.on("SIGTERM", gracefulShutdown)
}

// Start consuming the queue
consumeNotificationQueue().catch((error) => {
    console.error("Error in consuming notifications:", error)
    process.exit(1)
})
