import sqlite3 from "better-sqlite3";
import retry from "fetch-retry";
import Queue from "queue";

const db = new sqlite3("hn.db");

const fetch = retry(global.fetch, {
  retries: 5,
  retryDelay: 1000,
});

function setupSqlite() {
  db.exec(
    `
      CREATE TABLE IF NOT EXISTS hn (
        id INTEGER PRIMARY KEY,
        type TEXT,
        title TEXT,
        url TEXT,
        score INTEGER,
        by TEXT,
        time INTEGER
      )
    `
  );
}

function getMaxId(): number {
  const stmt = db.prepare("SELECT MAX(id) as max_id FROM hn");
  const res = stmt.get();
  return (res as any)?.max_id ?? 0;
}

async function fetchJson(url: string): Promise<any> {
  const res = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
    },
  });
  return res.json();
}

async function getItem(id: number): Promise<any> {
  return await fetchJson(
    `https://hacker-news.firebaseio.com/v0/item/${id}.json`
  );
}

function safeFormatTime(t: number) {
  try {
    return new Date(t * 1000).toISOString();
  } catch (error) {
    console.log("bad time:", t);
  }
  return "";
}

let t1 = Date.now();

async function fetchAndInsertItem(id: number) {
  try {
    const item = await getItem(id);

    if (id % 10000 === 0) {
      console.log(
        `[${new Date().toLocaleTimeString()}] id: ${id}, at: ${safeFormatTime(
          item.time
        )}, cost: ${(Date.now() - t1) / 1000}s`
      );
      t1 = Date.now();
    }

    if (item?.type !== "story" || item?.score < 5) {
      return;
    }
    const stmt = db.prepare(
      `INSERT INTO hn (id, type, title, url, score, by, time) VALUES (?, ?, ?, ?, ?, ?, ?)`
    );
    const res = stmt.run(
      item.id,
      item.type,
      item.title,
      item.url,
      item.score,
      item.by,
      item.time
    );
    // console.log(res)
  } catch (error) {
    console.log(error);
    exit(1);
  }
}

const CONCURRENCY = 100;
const MAX_ID = 43847069; // 2025.4.30

async function dump() {
  // continue from max id
  const start = getMaxId() + 1;

  const queue = new Queue({
    concurrency: CONCURRENCY,
    autostart: true,
  });
  queue.addEventListener("error", (err) => {
    console.error("queue error:", err);
  });

  let i = start;
  while (i < MAX_ID) {
    while (queue.length > CONCURRENCY * 2) {
      await new Promise((resolve) => {
        setTimeout(resolve, 500);
      });
    }

    const ii = i;
    queue.push((cb) => {
      fetchAndInsertItem(ii)
        .then(() => {
          if (cb) cb(undefined, ii);
        })
        .catch((err) => {
          if (cb) cb(err, undefined);
        });
    });
    i += 1;
  }
}

function exit(code: number) {
  db.close();
  process.exit(code);
}

async function main() {
  setupSqlite();
  await dump();
}

main()
  .then(() => {
    console.log("Done!");
  })
  .catch((err) => {
    console.error(err);
  })
  .finally(() => {
    exit(0);
  });
