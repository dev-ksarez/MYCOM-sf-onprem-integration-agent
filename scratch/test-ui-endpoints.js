const http = require('http');
const fs = require('fs');
const path = require('path');

const BASE_URL = 'http://localhost:8080';
const WORKSPACE_DIR = '/Users/karstensarez/Documents/Projekte/sf-onprem-integration-agent';
const TEST_PROJECT_ID = 'test-project-123';

// Helper to perform HTTP requests
function request(options, body = null) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          body: data
        });
      });
    });

    req.on('error', (err) => {
      reject(err);
    });

    if (body) {
      req.write(typeof body === 'string' ? body : JSON.stringify(body));
    }
    req.end();
  });
}

// Parse cookies from Set-Cookie header
function parseCookies(setCookieHeaders) {
  const cookies = {};
  if (!setCookieHeaders) return cookies;
  const headers = Array.isArray(setCookieHeaders) ? setCookieHeaders : [setCookieHeaders];
  for (const header of headers) {
    const parts = header.split(';')[0].split('=');
    if (parts.length === 2) {
      cookies[parts[0].trim()] = parts[1].trim();
    }
  }
  return cookies;
}

// Build Cookie header string from cookie object
function buildCookieHeader(cookies) {
  return Object.entries(cookies)
    .map(([k, v]) => `${k}=${v}`)
    .join('; ');
}

async function runTests() {
  console.log('Starting integration test for project-specific logo endpoints...');

  const cookies = {};
  let csrfToken = '';

  // 1. GET / to get initial CSRF Token
  console.log('\n--- Step 1: GET / (fetch initial CSRF token) ---');
  const step1 = await request({
    host: 'localhost',
    port: 8080,
    path: '/',
    method: 'GET'
  });

  console.log('GET / Status:', step1.statusCode);
  const step1Cookies = parseCookies(step1.headers['set-cookie']);
  Object.assign(cookies, step1Cookies);
  csrfToken = cookies['sf_agent_csrf'];
  console.log('Cookies retrieved:', cookies);
  console.log('CSRF Token:', csrfToken);

  if (!csrfToken) {
    throw new Error('Failed to get CSRF token in Step 1');
  }

  // 2. POST /auth/login to authenticate
  console.log('\n--- Step 2: POST /auth/login ---');
  const loginPayload = {
    username: 'admin',
    password: 'admin123!'
  };
  const step2 = await request({
    host: 'localhost',
    port: 8080,
    path: '/auth/login',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Cookie': buildCookieHeader(cookies),
      'X-CSRF-Token': csrfToken
    }
  }, loginPayload);

  console.log('POST /auth/login Status:', step2.statusCode);
  console.log('POST /auth/login Body:', step2.body);
  const step2Cookies = parseCookies(step2.headers['set-cookie']);
  Object.assign(cookies, step2Cookies);
  console.log('Cookies after login:', cookies);

  if (step2.statusCode !== 200) {
    throw new Error(`Login failed with status ${step2.statusCode}`);
  }

  // 3. POST /api/admin/settings/logo with project-specific content
  console.log(`\n--- Step 3: POST /api/admin/settings/logo for project: ${TEST_PROJECT_ID} ---`);
  // Simple transparent 1x1 pixel PNG
  const dummyBase64Png = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=';
  const logoPayload = {
    fileName: 'test-logo.png',
    contentBase64: dummyBase64Png,
    projectId: TEST_PROJECT_ID
  };

  const step3 = await request({
    host: 'localhost',
    port: 8080,
    path: '/api/admin/settings/logo',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Cookie': buildCookieHeader(cookies),
      'X-CSRF-Token': csrfToken
    }
  }, logoPayload);

  console.log('Upload Logo Status:', step3.statusCode);
  console.log('Upload Logo Body:', step3.body);

  if (step3.statusCode !== 200) {
    throw new Error(`Logo upload failed with status ${step3.statusCode}`);
  }

  // Check if custom logo file exists in workspace
  const expectedPath = path.resolve(WORKSPACE_DIR, `data/custom-logo-${TEST_PROJECT_ID}.png`);
  console.log('Checking logo file path:', expectedPath);
  if (fs.existsSync(expectedPath)) {
    console.log('Success: custom-logo-[projectId].png exists in data/ directory.');
  } else {
    throw new Error('Failure: custom-logo-[projectId].png was NOT found in data/ directory.');
  }

  // 4. GET /assets/custom-logo?projectId=test-project-123
  console.log(`\n--- Step 4: GET /assets/custom-logo?projectId=${TEST_PROJECT_ID} ---`);
  const step4 = await request({
    host: 'localhost',
    port: 8080,
    path: `/assets/custom-logo?projectId=${TEST_PROJECT_ID}`,
    method: 'GET'
  });

  console.log('GET /assets/custom-logo Status:', step4.statusCode);
  console.log('GET /assets/custom-logo Content-Type:', step4.headers['content-type']);
  
  if (step4.statusCode !== 200 || step4.headers['content-type'] !== 'image/png') {
    throw new Error(`Logo serving failed: status ${step4.statusCode}, content-type ${step4.headers['content-type']}`);
  }

  // 5. DELETE /api/admin/settings/logo for project
  console.log(`\n--- Step 5: DELETE /api/admin/settings/logo for project: ${TEST_PROJECT_ID} ---`);
  const step5 = await request({
    host: 'localhost',
    port: 8080,
    path: `/api/admin/settings/logo?projectId=${TEST_PROJECT_ID}`,
    method: 'DELETE',
    headers: {
      'Cookie': buildCookieHeader(cookies),
      'X-CSRF-Token': csrfToken
    }
  });

  console.log('DELETE Logo Status:', step5.statusCode);
  console.log('DELETE Logo Body:', step5.body);

  if (step5.statusCode !== 200) {
    throw new Error(`Logo deletion failed with status ${step5.statusCode}`);
  }

  if (!fs.existsSync(expectedPath)) {
    console.log('Success: custom-logo-[projectId].png was successfully deleted.');
  } else {
    throw new Error('Logo deletion failed: custom-logo-[projectId].png still exists.');
  }

  // 6. GET /assets/custom-logo?projectId=test-project-123 fallback (should redirect to default)
  console.log(`\n--- Step 6: GET /assets/custom-logo?projectId=${TEST_PROJECT_ID} fallback redirect ---`);
  const step6 = await request({
    host: 'localhost',
    port: 8080,
    path: `/assets/custom-logo?projectId=${TEST_PROJECT_ID}`,
    method: 'GET'
  });

  console.log('Fallback Status:', step6.statusCode);
  console.log('Fallback Location:', step6.headers['location']);

  if (step6.statusCode !== 302 || !step6.headers['location'].includes('MyCom_Logo')) {
    throw new Error(`Logo fallback redirect failed: status ${step6.statusCode}, location ${step6.headers['location']}`);
  }

  console.log('\nALL ENDPOINT TESTS PASSED SUCCESSFULLY! ✅');
}

runTests().catch((err) => {
  console.error('Test failed with error:', err);
  process.exit(1);
});
