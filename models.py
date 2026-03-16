from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

# 사용자(직원)
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    name = db.Column(db.String(60), nullable=False)
    role = db.Column(db.String(20), default="staff")  # 'admin' or 'staff'
    password_hash = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, raw):
        self.password_hash = generate_password_hash(raw)

    def check_password(self, raw):
        return check_password_hash(self.password_hash, raw)

# 매물
class Listing(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    # 기본
    title = db.Column(db.String(200), nullable=False)
    category = db.Column(db.String(30), nullable=False)    # 아파트/오피스텔/빌라/원룸/상가/토지…
    trade_type = db.Column(db.String(20), nullable=False)  # 매매/전세/월세/단기임대/분양
    sale_price = db.Column(db.Integer, nullable=True)      # 매매가
    deposit = db.Column(db.Integer, nullable=True)         # 보증금
    rent = db.Column(db.Integer, nullable=True)            # 월세
    maintenance_fee = db.Column(db.Integer, nullable=True) # 관리비
    has_loan = db.Column(db.Boolean, default=False)        # 융자 여부

    # 면적/구조
    exclusive_m2 = db.Column(db.Float, nullable=True)  # 전용
    supply_m2 = db.Column(db.Float, nullable=True)     # 공급
    rooms = db.Column(db.Integer, default=0)
    baths = db.Column(db.Integer, default=0)
    structure = db.Column(db.String(30), default="")   # 원룸/투룸/쓰리룸/포룸/주인세대 등
    floor = db.Column(db.Integer, default=0)
    direction = db.Column(db.String(10), default="")   # 남향 등

    # 건물/설비
    built_year = db.Column(db.Integer, nullable=True)
    heating_type = db.Column(db.String(30), default="")
    has_elevator = db.Column(db.Boolean, default=False)
    has_parking = db.Column(db.Boolean, default=False)
    balcony = db.Column(db.Boolean, default=False)  # 베란다 유무

    # 주소
    road_addr = db.Column(db.String(200), default="")
    jibun_addr = db.Column(db.String(200), default="")
    dong = db.Column(db.String(50), default="")
    ho = db.Column(db.String(50), default="")

    # 좌표 (카카오맵 핀)
    lat = db.Column(db.Float, nullable=True)
    lng = db.Column(db.Float, nullable=True)

    # 옵션/설명/메모
    options = db.Column(db.String(500), default="")   # 콤마구분: 냉장고,세탁기,에어컨 …
    features = db.Column(db.String(200), default="")  # 올수리, 역세권 등 요약 태그
    description = db.Column(db.Text, default="")
    memo = db.Column(db.Text, default="")             # 내부 메모(직거래 주의 등)

    # 운영
    status = db.Column(db.String(20), default="pending")  # pending/approved/rejected
    hidden = db.Column(db.Boolean, default=False)
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    images = db.relationship(
        "ListingImage",
        backref="listing",
        cascade="all,delete-orphan",
        order_by="ListingImage.sort_order, ListingImage.uploaded_at"
    )

    # -------- Address/ho normalizers & sort helpers --------
    @staticmethod
    def _extract_jibun_numbers(addr: str):
        """Return (main, sub) 숫자 튜플. 예: '호산동 358-11' -> (358, 11).
        주소 내 마지막 숫자 패턴을 기준으로 파싱. 없으면 (0, 0).
        """
        import re
        if not addr:
            return (0, 0)
        # 마지막 숫자 블록(본번-부번) 탐색
        m = None
        for m in re.finditer(r"(\d+)(?:-(\d+))?", addr):
            pass
        if not m:
            return (0, 0)
        main = int(m.group(1)) if m.group(1) else 0
        sub = int(m.group(2)) if m.group(2) else 0
        return (main, sub)

    @staticmethod
    def _extract_dong(addr: str):
        """주소 문자열에서 동명을 추출 (없으면 '')."""
        import re
        if not addr:
            return ""
        # 가장 마지막 "~~동" 패턴을 포착
        cand = ""
        for m in re.finditer(r"([가-힣]+동)", addr):
            cand = m.group(1)
        return cand

    @property
    def normalized_ho(self):
        """'203호호'와 같은 중복을 '203호'로 정규화. 숫자 없으면 원문 유지."""
        import re
        raw = (self.ho or "").strip()
        if not raw:
            return ""
        m = re.search(r"(\d+)", raw)
        if not m:
            # 숫자가 없다면 뒤쪽 '호'는 한 번만 보장
            return raw.rstrip('호') + ('호' if not raw.endswith('호') else '')
        num = m.group(1)
        return f"{int(num)}호"

    @property
    def display_address(self):
        """지번 우선 표기. 지번 없으면 도로명."""
        return (self.jibun_addr or self.road_addr or "").strip()

    @property
    def jibun_sort_key(self):
        """지번 정렬용 키: (동, 본번, 부번). 동이 비어있으면 주소에서 추출."""
        dong = (self.dong or self._extract_dong(self.jibun_addr) or "")
        main, sub = self._extract_jibun_numbers(self.jibun_addr or "")
        # 사전식 정렬 안정성을 위해 소문자 처리
        return (dong, main, sub)

    # helper
    @property
    def exclusive_py(self):
        return round(self.exclusive_m2 / 3.3058, 2) if self.exclusive_m2 else None

    @property
    def supply_py(self):
        return round(self.supply_m2 / 3.3058, 2) if self.supply_m2 else None

    @property
    def price_per_py(self):
        # 평당가(매매가 기준, 없으면 보증금+월세*100 단순환산 예시)
        total = None
        if self.sale_price:
            total = self.sale_price
        elif self.deposit or self.rent:
            total = (self.deposit or 0) + (self.rent or 0) * 100
        if total and self.exclusive_py:
            return int(total / max(self.exclusive_py, 0.01))
        return None

    @property
    def display_name(self):
        has_title = bool(self.title)
        nh = self.normalized_ho
        if has_title and nh:
            return f"{self.title} {nh}"
        elif has_title:
            return self.title
        elif nh:
            return nh
        else:
            return ""

class ListingImage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    listing_id = db.Column(db.Integer, db.ForeignKey("listing.id"), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    sort_order = db.Column(db.Integer, nullable=False, default=0)
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)