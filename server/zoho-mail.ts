import nodemailer from 'nodemailer';

// Create Zoho Mail SMTP transporter
export async function createZohoTransporter() {
  // Use environment variables for security
  const email = process.env.ZOHO_EMAIL || 'info@licenseiq.ai';
  const password = process.env.ZOHO_PASSWORD;
  
  if (!password) {
    throw new Error('ZOHO_PASSWORD environment variable is required');
  }

  // Configure Zoho SMTP for organization accounts
  const transporter = nodemailer.createTransport({
    host: 'smtppro.zoho.com', // For paid Zoho Mail organization accounts
    port: 465,
    secure: true, // Use SSL
    auth: {
      user: email,
      pass: password,
    },
    // Additional options for better deliverability
    tls: {
      rejectUnauthorized: true,
    },
  });

  // Verify connection
  try {
    await transporter.verify();
    console.log('✅ Zoho Mail SMTP connection verified');
  } catch (error) {
    console.error('❌ Zoho Mail SMTP connection failed:', error);
    throw error;
  }

  return transporter;
}

// Send email using Zoho Mail
export async function sendZohoEmail(options: {
  to: string | string[];
  subject: string;
  html: string;
  from?: string;
}) {
  const transporter = await createZohoTransporter();
  const fromEmail = process.env.ZOHO_EMAIL || 'info@licenseiq.ai';
  
  const mailOptions = {
    from: options.from || `"LicenseIQ" <${fromEmail}>`,
    to: Array.isArray(options.to) ? options.to.join(', ') : options.to,
    subject: options.subject,
    html: options.html,
  };

  const info = await transporter.sendMail(mailOptions);
  return info;
}
