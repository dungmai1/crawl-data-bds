CREATE TABLE users (
    id UUID PRIMARY KEY,
    phone VARCHAR(20) UNIQUE,
    email VARCHAR(255) UNIQUE,
    password VARCHAR(255) NOT NULL,
    bio VARCHAR(500),
    full_name VARCHAR(255),
    avatar_url TEXT,
    is_verified BOOLEAN DEFAULT FALSE,
    is_pro_seller BOOLEAN DEFAULT FALSE,
    area VARCHAR(255),
    refresh_token_hash TEXT,
    refresh_token_expired_at TIMESTAMPTZ,
    status VARCHAR(20) CHECK (status IN ('ACTIVE','INACTIVE','BANNED')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_login_at TIMESTAMPTZ
);

CREATE TABLE roles (
    id UUID PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(50) UNIQUE,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE user_roles (
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    role_id UUID REFERENCES roles(id) ON DELETE CASCADE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE property_types (
    id UUID PRIMARY KEY,

    name VARCHAR(100),
    slug VARCHAR(100) UNIQUE,
    listing_type VARCHAR(10),

    attributes_schema JSONB
);

CREATE TABLE listings (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    property_type_id UUID REFERENCES property_types(id),
    title VARCHAR(255),
    description TEXT,

    price BIGINT NOT NULL CHECK (price >= 0),
    area_m2 DECIMAL(10,2),

    full_address VARCHAR(255),
    legal_document VARCHAR(50),

    province VARCHAR(100),
    ward VARCHAR(100),

    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),

    status VARCHAR(20) CHECK (status IN ('DRAFT','PENDING','ACTIVE','EXPIRED','REJECTED','SOLD')),

    view_count INT DEFAULT 0,
    phone_view INT DEFAULT 0,
    chat_count INT DEFAULT 0,

    image_urls TEXT[],
    attributes JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    expired_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ
);

CREATE TABLE favorites (
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    listing_id UUID REFERENCES listings(id) ON DELETE CASCADE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (user_id, listing_id)
);

CREATE TABLE packages (
    id UUID PRIMARY KEY,

    name VARCHAR(255) NOT NULL,
    code VARCHAR(50) UNIQUE NOT NULL,

    description TEXT,

    price BIGINT NOT NULL CHECK (price >= 0),
    duration_days INT NOT NULL CHECK (duration_days > 0),

    boost_type VARCHAR(50),
    listing_limit INT CHECK (listing_limit >= 0),

    is_active VARCHAR(20) CHECK (is_active IN ('ACTIVE','INACTIVE', 'PENDING')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expired_at TIMESTAMPTZ
);

CREATE TABLE user_packages (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    package_id UUID NOT NULL,
    listings_used INT NOT NULL DEFAULT 0 CHECK (listings_used >= 0),
    started_at TIMESTAMPTZ,
    expired_at TIMESTAMPTZ,
    status VARCHAR(20) CHECK (status IN ('ACTIVE','INACTIVE', 'EXPIRED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_user
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_package
        FOREIGN KEY (package_id) REFERENCES packages(id)
        ON DELETE RESTRICT
);
