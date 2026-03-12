# Playwright - Inspect Page Item ID Extractor

## โครงสร้างไฟล์

```
my-project/
├── tests/
│   └── test-inspect.spec.ts   ← script หลัก
├── playwright.config.ts
├── results/
│   └── item-ids.json          ← ผลลัพธ์ Item IDs
├── screenshots/               ← screenshot แต่ละขั้นตอน
└── xray-results/
    └── results.xml            ← สำหรับ upload Xray
```

## วิธีใช้

### 1. ติดตั้ง dependencies

```bash
npm init -y
npm install --save-dev @playwright/test typescript
npx playwright install chromium
```

### 2. วางไฟล์

```bash
mkdir tests
cp test-inspect.spec.ts tests/
```

### 3. รัน test

```bash
# รันปกติ (headless)
npx playwright test

# รูปแบบ headed ดูหน้าจอได้
npx playwright test --headed

# debug mode (หยุดทุก step)
npx playwright test --debug
```

### 4. ดูผลลัพธ์

- **Item IDs** → `results/item-ids.json`
- **Screenshots** → `screenshots/`
- **HTML Report** → `playwright-report/index.html`

```bash
npx playwright show-report
```

## ตัวอย่าง output (item-ids.json)

```json
{
  "timestamp": "2025-02-27T10:00:00.000Z",
  "api_url": "http://ai-universal-service-new...",
  "total_items": 20,
  "item_ids": [
    "abc123",
    "def456",
    ...
  ]
}
```

## Troubleshooting

| ปัญหา | แก้ไข |
|-------|-------|
| หา input field ไม่เจอ | ดูไฟล์ `debug-page.html` เพื่อดู HTML structure |
| เชื่อมต่อ URL ไม่ได้ | ตรวจสอบ VPN / network ว่าเข้าถึง preprod ได้ |
| Item IDs = 0 | ดู screenshots/03-result-loaded.png ว่า response มีอะไร |
