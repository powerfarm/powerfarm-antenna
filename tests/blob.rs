//! Journey B: POST /blob -> bucket -> Receipt references object -> Capability observes it.

mod common;

#[tokio::test]
async fn blob_goes_to_the_bucket_and_the_receipt_only_references_it() {
    let h = common::start("blob", &[]).await;

    let pdf = b"%PDF-1.7\n1 0 obj\n<< /Type /Page >>\nendobj\n2 0 obj\n<< /Type /Page >>\nendobj\n%%EOF\n";
    let res: serde_json::Value = common::client()
        .post(format!("{}/blob", h.base))
        .header("content-type", "application/pdf")
        .body(pdf.to_vec())
        .send().await.unwrap().json().await.unwrap();

    assert_eq!(res["status"], "completed");
    let digest = res["digest"].as_str().unwrap().to_string();
    assert_eq!(digest, blake3::hash(pdf).to_hex().to_string());

    let obs = &res["result"]["observation"];
    assert_eq!(obs["sniffed_content_type"], "application/pdf");
    assert_eq!(obs["size_bytes"], pdf.len());
    assert_eq!(obs["structure"]["page_markers"], 2);

    // Content-addressed on disk, under the fan-out path.
    let on_disk = h.dir.join("objects").join(&digest[0..2]).join(&digest[2..4]).join(&digest);
    assert!(on_disk.exists(), "object must exist at {}", on_disk.display());
    assert_eq!(std::fs::read(&on_disk).unwrap(), pdf);

    // SQLite holds the reference, not the bytes.
    let id = res["receipt_id"].as_str().unwrap();
    let one: serde_json::Value = common::client()
        .get(format!("{}/receipts/{id}", h.base))
        .send().await.unwrap().json().await.unwrap();
    assert_eq!(one["receipt"]["raw_storage"], "object");
    assert_eq!(one["receipt"]["raw_ref"], digest);
    assert_eq!(one["receipt"]["interaction"], "blob");

    // ...and the bytes are still served back verbatim.
    let raw = common::client()
        .get(format!("{}/receipts/{id}/raw", h.base))
        .send().await.unwrap().bytes().await.unwrap();
    assert_eq!(raw.as_ref(), pdf.as_slice());
}

#[tokio::test]
async fn identical_bytes_collapse_to_one_object() {
    let h = common::start("blob-dedupe", &[]).await;
    let body = b"the same bytes twice".to_vec();

    let mut digests = vec![];
    for _ in 0..2 {
        let res: serde_json::Value = common::client()
            .post(format!("{}/blob", h.base))
            .header("content-type", "text/plain")
            .body(body.clone())
            .send().await.unwrap().json().await.unwrap();
        digests.push(res["digest"].as_str().unwrap().to_string());
    }
    assert_eq!(digests[0], digests[1]);

    // Two receipts (two arrivals are two facts), one object.
    let count = walk_count(&h.dir.join("objects"));
    assert_eq!(count, 1, "content-addressed storage must not duplicate identical bytes");
}

fn walk_count(dir: &std::path::Path) -> usize {
    let mut n = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                if p.file_name().map(|f| f == "staging").unwrap_or(false) { continue; }
                n += walk_count(&p);
            } else {
                n += 1;
            }
        }
    }
    n
}

#[tokio::test]
async fn oversized_blobs_are_refused_before_they_are_preserved() {
    let dir = common::temp_dir("blob-limit");
    let mut cfg = common::config_for(&dir, &[]);
    cfg.server.max_blob_bytes = 1024;

    let app = antenna::build(cfg, &common::routes_path()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = antenna::http_router(app);
    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await.unwrap();
    });

    let res = common::client()
        .post(format!("http://{addr}/blob"))
        .body(vec![b'x'; 4096])
        .send().await.unwrap();

    assert_eq!(res.status(), 413);
    // Nothing was kept: bound comes before preserve.
    assert_eq!(walk_count(&dir.join("objects")), 0);
}
