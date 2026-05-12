import { test, expect } from '@playwright/test';
import * as fs from 'fs';

const INSPECT_URL = 'http://ai-webview-metadata.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/inspect';

// ── เพิ่ม request ได้เรื่อยๆ ตรงนี้ ─────────────────────────────────────────
const API_REQUESTS = [
  {
    name: 'ugcsfv-search',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=sfv&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=jB2Wmx1WpOLZ&content_type_filtering=ugcsfv&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fugcsfv&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fugcsfv',
  },
  {
    name: 'ecommerce-search',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=ecommerce&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=8E5jey9KkD3n&content_type_filtering=ecommerce&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fecommerce&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fecommerce',
  },
  {
    name: 'sfvseries-search-v1',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=sfv&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=4a93zWKQvANj&content_type_filtering=sfv_series&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fsfv_series&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fsfv_series',
  },
  {
    name: 'sfvseries-search-v2',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=sfvseries&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=4a93zWKQvANj&content_type_filtering=sfv_series&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fsfv_series&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fsfv_series',
  },
  {
    name: 'livetv-search-v1',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=top_results&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=2APVEbbo4zmX&content_type_filtering=livetv&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Flivetv&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Flivetv',
  },
  {
    name: 'livetv-search-v2',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=watch&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=2APVEbbo4zmX&content_type_filtering=livetv&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Flivetv&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Flivetv',
  },
    {
    name: 'movie-search-v1',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=watch&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=Pb6dQoonrOAp&content_type_filtering=movie&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fmovie&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fmovie',
  },
  {
    name: 'movie-search-v2',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=top_results&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=Pb6dQoonrOAp&content_type_filtering=movie&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fmovie&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fmovie',
  },
   {
    name: 'sportclip-search-v1',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=watch&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=0DEBKEx1xJ9&content_type_filtering=sport_clip&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fsport_clip&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fsport_clip',
  },
  {
    name: 'sportclip-search-v2',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=top_results&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=0DEBKEx1xJ9&content_type_filtering=sport_clip&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fsport_clip&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fsport_clip',
  },
  {
    name: 'gameitem-search-v1',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=game&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=08xmbPp23zyM&content_type_filtering=game_item&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fgame_item&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fgame_item',
  },
  {
    name: 'gameitem-search-v2',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=top_results&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=0Ge4ERr9edO5&content_type_filtering=game_item&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fgame_item&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fgame_item',
  },
    {
    name: 'gamearticle-search-v1',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=read&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=2gr1X4XWvmxq&content_type_filtering=game_article&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fgame_article&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fgame_article',
  },
  {
    name: 'gamearticle-search-v2',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=top_results&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=2gr1X4XWvmxq&content_type_filtering=game_article&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fgame_article&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fgame_article',
  },
  {
    name: 'coupon-search',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=top_results&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=1GJBJVpLL3OG&content_type_filtering=coupon&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fcoupon&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fcoupon',
  },
  {
    name: 'privilege-search-v1',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=privilege&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=YmNGN1dP78G3&content_type_filtering=privilege&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fprivilege&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fprivilege',
  },
  {
    name: 'privilege-search-v2',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=top_results&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=YmNGN1dP78G3&content_type_filtering=privilege&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fprivilege&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fprivilege',
  },
  {
    name: 'channel-search',
    url: 'http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/search-basic?language=th&cursor=1&search_type=channel&search_time_out=5s&option_rerank_by_time_off=false&ssoId=22092422&search_url=https%3A%2F%2Fai-raas-api.trueid-preprod.net%2Fpersonalize-rcom%2Fv2%2Fsearch-api%2Fapi%2Fv5%2Ftext_search&limit=100&id=23Pmn7w4xlQG&content_type_filtering=channel&validate_metadata_similar_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fchannel&metadata_item_url=http%3A%2F%2Fai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th%2Fmetadata%2Fchannel',
  }
  // ── เพิ่ม request ใหม่ได้ตรงนี้ ──
  // {
  //   name: 'my-new-request',
  //   url: 'http://...',
  // },
];

// ── วน test แต่ละ request อัตโนมัติ ─────────────────────────────────────────
for (const apiRequest of API_REQUESTS) {
  test(`Extract items: ${apiRequest.name}`, async ({ page }) => {
    fs.mkdirSync(`screenshots/${apiRequest.name}`, { recursive: true });
    fs.mkdirSync('results', { recursive: true });

    // ── 1. เปิดหน้า inspect ────────────────────────────────────────────────
    console.log(`\n🌐 [${apiRequest.name}] Opening inspect page...`);
    await page.goto(INSPECT_URL, { waitUntil: 'networkidle', timeout: 30_000 });
    await page.screenshot({ path: `screenshots/${apiRequest.name}/01-opened.png`, fullPage: true });

    // ── 2. เลือก Pre-Production ────────────────────────────────────────────
    await page.locator('input[type="radio"][value="preprod"]').click();
    console.log(`🔘 [${apiRequest.name}] Selected: Pre-Production`);
    await page.screenshot({ path: `screenshots/${apiRequest.name}/02-preprod.png`, fullPage: true });

    // ── 3. ใส่ URL ────────────────────────────────────────────────────────
    const urlInput = page.locator('input[type="text"]').first();
    await urlInput.click({ clickCount: 3 });
    await urlInput.fill(apiRequest.url);
    console.log(`📋 [${apiRequest.name}] URL filled`);

    // ── 4. กดปุ่ม "ดึงข้อมูล" ───────────────────────────────────────────
    await page.locator('button:has-text("ดึงข้อมูล")').click();
    console.log(`🔍 [${apiRequest.name}] Clicked "ดึงข้อมูล"`);

    // ── 5. รอผลโหลด ──────────────────────────────────────────────────────
    await page.waitForTimeout(6_000);
    await page.screenshot({ path: `screenshots/${apiRequest.name}/03-result.png`, fullPage: true });

    // ── 6. Scroll ลงเพื่อโหลด card ทั้งหมด ───────────────────────────────
    console.log(`📜 [${apiRequest.name}] Scrolling to load all cards...`);
    let prevCount = 0;
    for (let i = 0; i < 30; i++) {
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(1_500);
      const count = await page.locator('text=/^ID:.*$/').count();
      console.log(`  Scroll ${i + 1}: ${count} cards`);
      if (count === prevCount && count > 0) {
        console.log(`✅ All cards loaded: ${count}`);
        break;
      }
      prevCount = count;
    }

    await page.screenshot({ path: `screenshots/${apiRequest.name}/04-all-cards.png`, fullPage: true });

    // ── 7. ดึง ID + content_type จากทุก card ─────────────────────────────
    const items = await page.evaluate(() => {
      const results: { id: string; content_type: string }[] = [];
      const allElements = Array.from(document.querySelectorAll('*'));

      const idElements = allElements.filter(el => {
        const text = el.childNodes[0]?.textContent?.trim() || '';
        return text.startsWith('ID:') && el.children.length === 0;
      });

      idElements.forEach(idEl => {
        const idMatch = idEl.textContent?.trim().match(/ID:\s*(\S+)/);
        if (!idMatch) return;
        const itemId = idMatch[1];

        let parent = idEl.parentElement;
        let contentType = '';
        for (let d = 0; d < 10 && parent; d++) {
          const ctMatch = parent.innerHTML?.match(/content-type:\s*([a-zA-Z0-9_-]+)/);
          if (ctMatch) { contentType = ctMatch[1]; break; }
          parent = parent.parentElement;
        }

        if (!results.find(r => r.id === itemId)) {
          results.push({ id: itemId, content_type: contentType || 'unknown' });
        }
      });

      return results;
    });

    // ── 8. แสดงผลใน console ──────────────────────────────────────────────
    console.log(`\n📦 [${apiRequest.name}] Found ${items.length} items`);
    console.log('No. | ID                  | content_type');
    console.log('----+---------------------+-------------');
    items.forEach((item, i) => {
      console.log(`${String(i + 1).padStart(3)} | ${item.id.padEnd(20)} | ${item.content_type}`);
    });

    // ── 9. บันทึกผลแยกไฟล์ตาม request name ──────────────────────────────
    const output = {
      timestamp: new Date().toISOString(),
      name: apiRequest.name,
      environment: 'Pre-Production',
      api_url: apiRequest.url,
      total_items: items.length,
      items,
    };
    fs.writeFileSync(`results/${apiRequest.name}.json`, JSON.stringify(output, null, 2));

    const csv = ['id,content_type', ...items.map(i => `${i.id},${i.content_type}`)].join('\n');
    fs.writeFileSync(`results/${apiRequest.name}.csv`, csv);

    console.log(`💾 Saved: results/${apiRequest.name}.json`);
    console.log(`💾 Saved: results/${apiRequest.name}.csv`);

    expect(items.length).toBeGreaterThan(0);
  });
}