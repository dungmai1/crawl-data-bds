-- ============================================================
-- Batch insert generated from: test_upload_r2.json
-- Generated at: 2026-04-17T18:16:59
-- Total listings: 1
-- Prerequisite: property_types.sql đã được chạy
--               unique index idx_listings_source_id đã tồn tại
-- ============================================================

-- [1/1] source=nhatot source_id=175327293
INSERT INTO listings (
    id,
    user_id,
    property_type_id,
    title,
    description,
    price,
    area_m2,
    full_address,
    legal_document,
    province,
    ward,
    latitude,
    longitude,
    status,
    image_urls,
    attributes,
    published_at
) VALUES (
    '9713cd0e-972f-44b4-bb3a-a9d50ba0ced0',
    NULL,
    (SELECT id FROM property_types WHERE slug = 'nha-mat-pho'),
    'Nhà 6mx30m Đúc 7 Tầng Có Thang Máy Mặt Tiền Nguyễn Thị Sóc, Hóc Môn',
    'Giảm sốc bán gấp đi mỹ
Nhà Mặt Tiền Nguyễn Thị Sóc, Bà Điểm, Hóc Môn
Diện tích : 6m x 22m tóp hậu. Công nhận 112m2 ( thực tế 6mx30m)
Sổ hồng hoàn công đầy đủ
Nhà 1 trệt 1 lửng 5 lầu. Có thang máy
Gồm 10 phòng , 11 toilet, sân thượng cực rộng
Ngay chợ đầu mối
Đường 40m vỉa hè 6m
Giá bán : 10.490 tỷ
Liên hệ duy xem nhà thúc tế',
    10490000000,
    112,
    'Đường Nguyễn Thị Sóc, Xã Bà Điểm, Thành phố Hồ Chí Minh',
    'so-hong',
    'Thành phố Hồ Chí Minh',
    'Xã Bà Điểm',
    10.8489485,
    106.600105,
    'active',
    ARRAY['https://pub-cf4638a4c1b244b6bbcc6e6d5b669249.r2.dev/properties/nhatot/175327293/0.jpg', 'https://pub-cf4638a4c1b244b6bbcc6e6d5b669249.r2.dev/properties/nhatot/175327293/1.jpg', 'https://pub-cf4638a4c1b244b6bbcc6e6d5b669249.r2.dev/properties/nhatot/175327293/2.jpg', 'https://pub-cf4638a4c1b244b6bbcc6e6d5b669249.r2.dev/properties/nhatot/175327293/3.jpg', 'https://pub-cf4638a4c1b244b6bbcc6e6d5b669249.r2.dev/properties/nhatot/175327293/4.jpg'],
    '{"source": "nhatot", "source_id": "175327293", "source_url": "https://www.nhatot.com/mua-ban-tp-hồ-chí-minh/131740227.htm", "bedrooms": 10, "bathrooms": 7, "floors": 7, "direction": "dong-nam", "price_per_m2": 93660714, "price_display": "10,49 tỷ", "street": "Đường Nguyễn Thị Sóc", "district_legacy": "Huyện Hóc Môn", "phone_full": "0985239435", "contact_name": "Nhà Đất Sài Gòn", "poster_type": "moi_gioi", "quality_score": 100, "scraped_at": "2026-04-14T16:51:12.244362"}'::jsonb,
    '2026-04-14T16:42:18'::timestamptz
) ON CONFLICT ((attributes->>'source_id'), (attributes->>'source')) DO NOTHING;
