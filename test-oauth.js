const dotenv = require('dotenv');
dotenv.config();

const loginUrl = (process.env.SF_LOGIN_URL || '').replace(/\/$/, '');

console.log('🔐 OAuth Test\n');
console.log('Login URL:', loginUrl);
console.log('Client ID:', (process.env.SF_CLIENT_ID || 'MISSING').substring(0, 20) + '...');
console.log('Client Secret:', (process.env.SF_CLIENT_SECRET || 'MISSING').substring(0, 20) + '...');
console.log('Username:', process.env.SF_USERNAME || 'MISSING');
console.log('Password:', (process.env.SF_PASSWORD || 'MISSING').substring(0, 20) + '...');

(async () => {
  const body = new URLSearchParams({
    grant_type: 'password',
    client_id: process.env.SF_CLIENT_ID || '',
    client_secret: process.env.SF_CLIENT_SECRET || '',
    username: process.env.SF_USERNAME || '',
    password: process.env.SF_PASSWORD || ''
  });

  console.log('\n📤 Sending request to:', `${loginUrl}/services/oauth2/token\n`);

  const resp = await fetch(`${loginUrl}/services/oauth2/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  });

  const text = await resp.text();
  console.log('Status:', resp.status);
  console.log('Response:', text);
  
  if (resp.ok) {
    const data = JSON.parse(text);
    console.log('\n✅ Success! Token:', data.access_token.substring(0, 30) + '...');
  }
})();
