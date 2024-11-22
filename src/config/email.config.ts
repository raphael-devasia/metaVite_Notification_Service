const nodemailer = require("nodemailer")
import dotenv from "dotenv"

// Load environment variables from .env file
dotenv.config()

// Create a transporter object using Gmail SMTP
export const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
        user: process.env.EMAIL_USER, // Your Gmail address
        pass: process.env.EMAIL_PASSWORD, // Your Gmail password or App Password
    },
})


