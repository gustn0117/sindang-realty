import os
from datetime import datetime
from flask import (
    Flask, render_template, render_template_string, request, redirect, url_for, flash, send_from_directory, jsonify, send_file
)
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename
from itsdangerous import URLSafeSerializer
from dotenv import load_dotenv

import re
from io import BytesIO
try:
    from pdfminer.high_level import extract_text
except Exception:
    extract_text = None  # pdfminer.six가 설치되지 않은 경우 대비

# Safe import for Pillow (treat as module, not class)
try:
    import importlib
    PIL_Image = importlib.import_module("PIL.Image")       # module object
    PIL_ImageOps = importlib.import_module("PIL.ImageOps") # for exif-based orientation fix
except Exception:
    PIL_Image = None
    PIL_ImageOps = None

from sqlalchemy import and_, or_

from config import Config
from models import db, User, Listing, ListingImage

load_dotenv()

app = Flask(__name__)
app.config.from_object(Config)
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024  # allow up to 256MB per request (≈50 images)

# folders
os.makedirs(os.path.join(os.path.dirname(__file__), "instance"), exist_ok=True)
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

# db / auth
db.init_app(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message_category = "warning"

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

# --- 공유 링크용 토큰 유틸 ---
def get_share_serializer():
    return URLSafeSerializer(app.config['SECRET_KEY'], salt="share-listing")

def generate_share_token(listing_id: int) -> str:
    s = get_share_serializer()
    return s.dumps({"id": listing_id})

def decode_share_token(token: str):
    s = get_share_serializer()
    try:
        data = s.loads(token)
        return data.get("id")
    except Exception:
        return None

# --- util ---
def allowed_file(filename):
    ext = filename.rsplit(".", 1)[-1].lower()
    return "." in filename and ext in app.config["ALLOWED_EXTENSIONS"]

def require_admin():
    return current_user.is_authenticated and current_user.role == "admin"

# --- safe_float helper ---
def safe_float(val):
    """Convert to float safely. Treat 'None', 'null', '' as missing."""
    try:
        if val in (None, "", "None", "null", "NULL"):
            return None
        return float(val)
    except (ValueError, TypeError):
        return None

# --- to_bool helper ---
def to_bool(val):
    """HTML checkbox -> bool. Accepts 'on', 'true', '1', True."""
    return str(val).lower() in ("on", "true", "1", "y", "yes")

# --- next_image_order helper ---
def next_image_order(listing_id: int) -> int:
    last = (
        db.session.query(ListingImage.sort_order)
        .filter(ListingImage.listing_id == listing_id)
        .order_by(ListingImage.sort_order.desc())
        .first()
    )
    return (last[0] + 1) if last else 0

# --- save_optimized_image helper ---
def save_optimized_image(file_storage, listing_id: int, upload_dir: str) -> str:
    """
    Save an uploaded image with size/quality optimization.
    Returns the saved filename.
    """
    # Default filename base
    base = secure_filename(f"{listing_id}_{datetime.utcnow().timestamp()}")
    # If Pillow is not available, save as-is
    if PIL_Image is None:
        fname = secure_filename(f"{base}_{file_storage.filename}")
        path = os.path.join(upload_dir, fname)
        file_storage.save(path)
        return fname

    # Try to process with Pillow
    try:
        file_storage.stream.seek(0)
        img = PIL_Image.open(file_storage.stream)

        # Fix orientation based on EXIF (so that portrait/landscape are saved as seen on phone)
        try:
            if 'PIL_ImageOps' in globals() and PIL_ImageOps is not None:
                img = PIL_ImageOps.exif_transpose(img)
        except Exception:
            # If EXIF is missing or Pillow version doesn't support it, just skip
            pass

        # Convert to RGB for JPEG/WebP
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGB")
        # Resize: cap long side at 1600px (keeps good quality, lowers size)
        max_side = 1600
        w, h = img.size
        if max(w, h) > max_side:
            if w >= h:
                nh = int(h * (max_side / float(w)))
                img = img.resize((max_side, nh))
            else:
                nw = int(w * (max_side / float(h)))
                img = img.resize((nw, max_side))

        # Prefer WebP if supported, otherwise JPEG
        # Use .webp to drastically reduce size; fallback to .jpg
        try:
            fname = f"{base}.webp"
            path = os.path.join(upload_dir, fname)
            img.save(path, format="WEBP", quality=80, method=6)
            return fname
        except Exception:
            fname = f"{base}.jpg"
            path = os.path.join(upload_dir, fname)
            img.save(path, format="JPEG", quality=85, optimize=True, progressive=True)
            return fname
    except Exception:
        # Any failure: save raw
        fname = secure_filename(f"{base}_{file_storage.filename}")
        path = os.path.join(upload_dir, fname)
        try:
            file_storage.stream.seek(0)
        except Exception:
            pass
        file_storage.save(path)
        return fname

# --- CLI 최초 기동용 (관리자 생성) ---
@app.cli.command("initdb")
def initdb():
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(email="sindang1234").first():
            admin = User(email="sindang1234", name="관리자", role="admin")
            admin.set_password("1234")
            db.session.add(admin)
            db.session.commit()
            print("✓ DB 초기화 및 관리자(sindang1234 / 1234) 생성")

# --- routes ---
@app.route("/")
@login_required
def home():
    return redirect(url_for("listings"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email","").strip().lower()
        password = request.form.get("password","")
        user = User.query.filter_by(email=email).first()
        if user and user.check_password(password):
            login_user(user)
            flash("로그인되었습니다.", "success")
            return redirect(url_for("listings"))
        flash("이메일 또는 비밀번호가 올바르지 않습니다.", "danger")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("로그아웃되었습니다.", "info")
    return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
def dashboard():
    # 승인 대기/승인/반려 카운트
    counts = {
        "pending": Listing.query.filter_by(status="pending").count(),
        "approved": Listing.query.filter_by(status="approved").count(),
        "rejected": Listing.query.filter_by(status="rejected").count(),
    }
    recent = Listing.query.order_by(Listing.created_at.desc()).limit(5).all()
    return render_template("dashboard.html", counts=counts, recent=recent)

# 리스트 + 필터 + 정렬
@app.route("/listings")
@login_required
def listings():
    q = Listing.query

    # 숨김 기본 제외 (show_hidden=1 이면 포함)
    show_hidden = (request.args.get("show_hidden") or "0").strip()
    if show_hidden != "1":
        try:
            q = q.filter(or_(Listing.hidden.is_(False), Listing.hidden.is_(None)))
        except Exception:
            # models에 hidden 컬럼이 아직 없을 경우를 대비한 안전장치
            pass

    # 경산시 필터 (only_gyeongsan=1 이면 경산시 주소만)
    only_gyeongsan = (request.args.get("only_gyeongsan") or "0").strip()
    if only_gyeongsan == "1":
        try:
            q = q.filter(
                or_(
                    Listing.road_addr.ilike("%경산%"),
                    Listing.jibun_addr.ilike("%경산%"),
                )
            )
        except Exception:
            # jibun_addr가 없거나 컬럼 접근 실패 시에도 최소한 road_addr만이라도 필터
            try:
                q = q.filter(Listing.road_addr.ilike("%경산%"))
            except Exception:
                pass

    # 권한: 내부용이므로 전체 보되, 필요시 승인만 보기 토글 가능
    status = request.args.get("status", "")  # "", approved, pending, rejected
    if status in ("approved", "pending", "rejected"):
        q = q.filter(Listing.status == status)

    # 지역 키워드(도로명/지번/동)
    region = (request.args.get("region") or request.args.get("q") or "").strip()
    if region:
        import re
        from urllib.parse import quote_plus
        import requests

        # 1) '동/읍/면' 토큰 추출 (예: '대구 신당동' → '신당동')
        tokens = [t for t in re.split(r"[\s,]+", region) if t]
        dong_kw = next((t for t in reversed(tokens) if re.search(r"[동읍면]$", t)), None)

        # 2) 상위 행정구역 토큰 추출을 위해 지오코딩 시도(대구/달서구 등)
        upper_tokens = []  # ['대구', '달서구'] 같은 리스트
        region_norm = region
        try:
            kakao_key = os.getenv('KAKAO_REST_API_KEY') or app.config.get('KAKAO_REST_API_KEY')
            if kakao_key:
                url = f'https://dapi.kakao.com/v2/local/search/address.json?query={quote_plus(region)}'
                headers = {'Authorization': f'KakaoAK {kakao_key}'}
                res = requests.get(url, headers=headers, timeout=3)
                if res.status_code == 200:
                    data = res.json()
                    if data.get('documents'):
                        addr_info = data['documents'][0]
                        # 정규화 주소
                        region_norm = (
                            (addr_info.get('road_address') or {}).get('address_name')
                            or (addr_info.get('address') or {}).get('address_name')
                            or region
                        )
                        # 상위 시/구 단위 토큰 (region_1depth_name, region_2depth_name)
                        road = addr_info.get('road_address') or {}
                        addr = addr_info.get('address') or {}
                        r1 = road.get('region_1depth_name') or addr.get('region_1depth_name')
                        r2 = road.get('region_2depth_name') or addr.get('region_2depth_name')
                        for t in (r1, r2):
                            if t and t not in upper_tokens:
                                upper_tokens.append(t)
        except Exception:
            pass

        # 3) 필터 구성
        if dong_kw:
            # (A) dong 일치 OR (B) 상위 토큰들이 모두 포함된 도로명/지번 주소
            like_dong = f"%{dong_kw}%"
            cond_dong = Listing.dong.ilike(like_dong)

            cond_addr = None
            for t in upper_tokens:
                tlike = f"%{t}%"
                token_cond = or_(Listing.road_addr.ilike(tlike), Listing.jibun_addr.ilike(tlike))
                cond_addr = token_cond if cond_addr is None else and_(cond_addr, token_cond)

            if cond_addr is not None:
                q = q.filter(or_(cond_dong, cond_addr))
            else:
                # 상위 토큰을 못 얻었으면: dong 또는 (도로명/지번) 에서 '동' 토큰 매칭
                q = q.filter(or_(cond_dong, Listing.road_addr.ilike(like_dong), Listing.jibun_addr.ilike(like_dong)))
        else:
            # dong 토큰이 없으면 정규화 문자열로 폭넓게 검색
            like = f"%{region_norm}%"
            q = q.filter(
                or_(
                    Listing.road_addr.ilike(like),
                    Listing.jibun_addr.ilike(like),
                    Listing.dong.ilike(like),
                    Listing.title.ilike(like)
                )
            )

    # 자주 이용하는 동 체크박스 필터 (?dongs=호산동&dongs=신당동 ...)
    selected_dongs = request.args.getlist("dongs")
    if selected_dongs:
        q = q.filter(Listing.dong.in_(selected_dongs))

    # 거래유형/매물종류
    trade_type = (request.args.get("trade_type", "") or "").strip()
    if trade_type and trade_type != "전체":
        q = q.filter(Listing.trade_type == trade_type)

    category = (request.args.get("category", "") or "").strip()
    if category and category != "전체":
        q = q.filter(Listing.category == category)

    # 구조(선택형: 전체는 미적용, 값이 있으면 정확 일치)
    structure_kw = (request.args.get("structure", "") or "").strip()
    if structure_kw and structure_kw != "전체":
        q = q.filter(Listing.structure == structure_kw)

    balcony_param = (request.args.get("balcony", "") or "").lower()
    if balcony_param in ("1", "true", "on", "y", "yes"):  # 체크된 경우만 True 필터
        q = q.filter(Listing.balcony.is_(True))
    elif balcony_param in ("0", "false", "off", "n", "no"):
        q = q.filter(Listing.balcony.is_(False))

    # 키워드(제목/특징/옵션/설명)
    keyword = request.args.get("keyword","").strip()
    if keyword:
        like = f"%{keyword}%"
        q = q.filter(
            (Listing.title.ilike(like)) |
            (Listing.features.ilike(like)) |
            (Listing.options.ilike(like)) |
            (Listing.description.ilike(like))
        )

    # 정렬
    sort = request.args.get("sort", "latest").strip()
    if sort == "price":
        q = q.order_by((Listing.sale_price.is_(None)).asc(), Listing.sale_price.asc())
        listings = q.all()
    elif sort == "jibun":
        # 파이썬 측 정렬: (동, 본번, 부번)
        listings = q.all()
        try:
            listings.sort(key=lambda l: l.jibun_sort_key)
        except Exception:
            pass
    else:  # latest
        q = q.order_by(Listing.created_at.desc())
        listings = q.all()

    return render_template("listings.html", listings=listings, map_key=app.config["KAKAO_MAP_KEY"])

# 등록/수정
@app.route("/listings/new", methods=["GET", "POST"])
@login_required
def listing_new():
    if request.method == "POST":
        data = request.form
        l = Listing(
            title=data.get("title","" ).strip(),
            category=data.get("category",""),
            trade_type=(data.get("trade_type","월세") or "월세"),
            sale_price=int(data["sale_price"]) if data.get("sale_price") else None,
            deposit=int(data["deposit"]) if data.get("deposit") else None,
            rent=int(data["rent"]) if data.get("rent") else None,
            maintenance_fee=int(data["maintenance_fee"]) if data.get("maintenance_fee") else None,
            structure=data.get("structure",""),
            road_addr=data.get("road_addr",""),
            dong=data.get("dong",""),
            ho=data.get("ho",""),
            lat=safe_float(data.get("lat")),
            lng=safe_float(data.get("lng")),
            options=",".join([o.strip() for o in data.get("options","").split(",") if o.strip()]),
            features=",".join([f.strip() for f in data.get("features","").split(",") if f.strip()]),
            description=data.get("description",""),
            memo=data.get("memo",""),
            balcony=to_bool(data.get("balcony")),
            created_by=current_user.id,
            status="pending" if not require_admin() else "approved"
        )
        # 호수 정규화: '203호호' 등 → '203호'
        try:
            import re as _re
            raw_ho = (l.ho or "").strip()
            m = _re.search(r"(\d+)", raw_ho)
            if m:
                l.ho = f"{int(m.group(1))}호"
            elif raw_ho:
                l.ho = raw_ho.rstrip('호') + ('호' if not raw_ho.endswith('호') else '')
        except Exception:
            pass
        try:
            l.hidden = False
        except Exception:
            pass
        db.session.add(l)
        db.session.commit()

        # 이미지 업로드 (sort_order 부여)
        files = request.files.getlist("images")
        order_cursor = next_image_order(l.id)
        for file in files[:50]:  # hard cap to 50 images per submission
            if file and allowed_file(file.filename):
                fname = save_optimized_image(file, l.id, app.config["UPLOAD_FOLDER"])
                db.session.add(ListingImage(listing_id=l.id, filename=fname, sort_order=order_cursor))
                order_cursor += 1

        # 업로드 후 정합성: sort_order 0..N으로 보정
        from sqlalchemy import select
        _imgs = db.session.scalars(
            select(ListingImage).where(ListingImage.listing_id == l.id).order_by(ListingImage.sort_order.asc(), ListingImage.uploaded_at.asc())
        ).all()
        for _i, _img in enumerate(_imgs):
            _img.sort_order = _i
        db.session.commit()

        flash("매물이 등록되었습니다. (승인 대기)" if l.status=="pending" else "매물이 등록되었습니다.", "success")
        return redirect(url_for("listing_detail", listing_id=l.id))

    # GET
    return render_template("listing_form.html", listing=None, map_key=app.config["KAKAO_MAP_KEY"])

@app.route("/listings/<int:listing_id>/edit", methods=["GET", "POST"])
@login_required
def listing_edit(listing_id):
    l = db.session.get(Listing, listing_id)
    if not l:
        flash("존재하지 않는 매물입니다.", "warning")
        return redirect(url_for("listings"))

    if request.method == "POST":
        data = request.form
        l.title = data.get("title","").strip()
        l.category = data.get("category","")
        l.trade_type = data.get("trade_type","")
        l.sale_price = int(data["sale_price"]) if data.get("sale_price") else None
        l.deposit = int(data["deposit"]) if data.get("deposit") else None
        l.rent = int(data["rent"]) if data.get("rent") else None
        l.maintenance_fee = int(data["maintenance_fee"]) if data.get("maintenance_fee") else None
        l.structure = data.get("structure","")
        l.road_addr = data.get("road_addr","")
        l.dong = data.get("dong","")
        l.ho = data.get("ho","")
        # 호수 정규화: '203호호' 등 → '203호'
        try:
            import re as _re
            raw_ho = (l.ho or "").strip()
            m = _re.search(r"(\d+)", raw_ho)
            if m:
                l.ho = f"{int(m.group(1))}호"
            elif raw_ho:
                l.ho = raw_ho.rstrip('호') + ('호' if not raw_ho.endswith('호') else '')
        except Exception:
            pass
        l.lat = safe_float(data.get("lat"))
        l.lng = safe_float(data.get("lng"))
        l.options = ",".join([o.strip() for o in data.get("options","").split(",") if o.strip()])
        l.features = ",".join([f.strip() for f in data.get("features","").split(",") if f.strip()])
        l.description = data.get("description","")
        l.memo = data.get("memo","")
        l.balcony = to_bool(data.get("balcony"))


        # 이미지 추가 업로드 (sort_order 부여)
        files = request.files.getlist("images")
        order_cursor = next_image_order(l.id)
        for file in files[:50]:  # hard cap to 50 images per submission
            if file and allowed_file(file.filename):
                fname = save_optimized_image(file, l.id, app.config["UPLOAD_FOLDER"])
                db.session.add(ListingImage(listing_id=l.id, filename=fname, sort_order=order_cursor))
                order_cursor += 1

        # 기존 이미지 삭제 처리 (숨겨진 입력 delete_images 사용)
        del_raw = (data.get("delete_images") or "").strip()
        if del_raw:
            del_ids = []
            for tok in del_raw.split(','):
                tok = tok.strip()
                if not tok:
                    continue
                try:
                    del_ids.append(int(tok))
                except ValueError:
                    pass
            if del_ids:
                from sqlalchemy import select
                imgs_to_del = db.session.scalars(
                    select(ListingImage).where(ListingImage.listing_id == l.id, ListingImage.id.in_(del_ids))
                ).all()
                for img in imgs_to_del:
                    try:
                        os.remove(os.path.join(app.config["UPLOAD_FOLDER"], img.filename))
                    except Exception:
                        pass
                    db.session.delete(img)

        # 정합성: 현재 이미지들을 sort_order 0..N으로 재배치
        from sqlalchemy import select
        current_imgs = db.session.scalars(
            select(ListingImage).where(ListingImage.listing_id == l.id).order_by(ListingImage.sort_order.asc(), ListingImage.uploaded_at.asc())
        ).all()
        for idx, img in enumerate(current_imgs):
            img.sort_order = idx

        db.session.commit()
        flash("수정되었습니다.", "success")
        return redirect(url_for("listing_detail", listing_id=l.id))

    # GET
    return render_template("listing_form.html", listing=l, map_key=app.config["KAKAO_MAP_KEY"])

@app.route("/listings/<int:listing_id>")
@login_required
def listing_detail(listing_id):
    l = db.session.get(Listing, listing_id)
    if not l:
        flash("존재하지 않는 매물입니다.", "warning")
        return redirect(url_for("listings"))

    # 내부에서 보는 상세 페이지: 공유용 URL을 함께 넘긴다.
    token = generate_share_token(l.id)
    share_url = url_for("listing_detail_public", token=token, _external=True)

    return render_template(
        "listing_detail.html",
        l=l,
        map_key=app.config["KAKAO_MAP_KEY"],
        is_public=False,
        share_url=share_url,
    )

# 공유용 공개 상세 페이지
@app.route("/p/<token>")
def listing_detail_public(token):
    listing_id = decode_share_token(token)
    if not listing_id:
        flash("유효하지 않은 링크입니다.", "warning")
        return redirect(url_for("login"))

    l = db.session.get(Listing, listing_id)
    if not l:
        flash("존재하지 않는 매물입니다.", "warning")
        return redirect(url_for("login"))

    # 로그인 없이 볼 수 있는 공유용 상세 페이지
    return render_template(
        "listing_detail.html",
        l=l,
        map_key=app.config["KAKAO_MAP_KEY"],
        is_public=True,
    )

@app.route("/listings/<int:listing_id>/delete", methods=["POST"])
@login_required
def listing_delete(listing_id):
    l = db.session.get(Listing, listing_id)
    if not l:
        flash("존재하지 않는 매물입니다.", "warning")
    else:
        # 이미지 파일 삭제
        for img in l.images:
            try:
                os.remove(os.path.join(app.config["UPLOAD_FOLDER"], img.filename))
            except Exception:
                pass
        db.session.delete(l)
        db.session.commit()
        flash("삭제되었습니다.", "info")
    return redirect(url_for("listings"))


# 숨김 토글
@app.route("/listings/<int:listing_id>/toggle_hidden", methods=["POST"])
@login_required
def toggle_hidden(listing_id):
    l = db.session.get(Listing, listing_id)
    if not l:
        flash("존재하지 않는 매물입니다.", "warning")
        return redirect(url_for("listings"))
    try:
        current = bool(getattr(l, "hidden", False))
        l.hidden = not current
        db.session.commit()
        flash("숨김 해제되었습니다." if current else "숨김 처리되었습니다.", "info")
    except Exception:
        flash("숨김 상태를 변경할 수 없습니다. 데이터베이스 스키마를 확인하세요.", "danger")
    return redirect(url_for("listing_detail", listing_id=listing_id))

# 숨김 상태 명시 설정 (목록 체크박스용)
@app.route("/listings/<int:listing_id>/set_hidden", methods=["POST"])
@login_required
def set_hidden(listing_id):
    """목록 화면의 체크박스에서 hidden 값을 명시적으로 설정하기 위한 엔드포인트.

    - form-data: hidden=1/0 또는 true/false
    - JSON: {"hidden": true/false}

    성공 시 JSON을 반환하고, 실패 시 적절한 에러 코드를 반환한다.
    """
    l = db.session.get(Listing, listing_id)
    if not l:
        return jsonify({"ok": False, "error": "not_found"}), 404

    payload = request.get_json(silent=True)
    if isinstance(payload, dict) and "hidden" in payload:
        hidden_val = payload.get("hidden")
        hidden_bool = to_bool(hidden_val) if isinstance(hidden_val, str) else bool(hidden_val)
    else:
        hidden_bool = to_bool(request.form.get("hidden"))

    try:
        l.hidden = bool(hidden_bool)
        db.session.commit()
        return jsonify({"ok": True, "id": l.id, "hidden": bool(getattr(l, "hidden", False))})
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "schema_or_db_error"}), 500

# 숨김 일괄 처리 (목록에서 여러 개 체크 후 처리)
@app.route("/listings/bulk_set_hidden", methods=["POST"])
@login_required
def bulk_set_hidden():
    """ids[] 목록을 받아 hidden 값을 일괄 설정한다.

    - form-data: ids[] = [1,2,3], hidden=1/0
    - JSON: {"ids": [1,2,3], "hidden": true/false}
    """
    payload = request.get_json(silent=True)

    if isinstance(payload, dict):
        ids = payload.get("ids") or []
        hidden_val = payload.get("hidden")
        hidden_bool = to_bool(hidden_val) if isinstance(hidden_val, str) else bool(hidden_val)
    else:
        ids = request.form.getlist("ids[]") or request.form.getlist("ids")
        hidden_bool = to_bool(request.form.get("hidden"))

    if not isinstance(ids, list) or not ids:
        return jsonify({"ok": True, "updated": 0})

    updated = 0
    for sid in ids:
        try:
            li = db.session.get(Listing, int(sid))
        except Exception:
            li = None
        if not li:
            continue
        try:
            li.hidden = bool(hidden_bool)
            updated += 1
        except Exception:
            # hidden 컬럼이 없으면 조용히 패스
            pass

    try:
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "db_error"}), 500

    return jsonify({"ok": True, "updated": updated, "hidden": bool(hidden_bool)})

# --- PDF 파싱 & 매물 동기화 ---
def parse_pdf_for_units(pdf_bytes: bytes):
    """PDF에서 (동, 지번, 호, 가격) 항목을 추출한다.

    온하우스 양식 PDF는 pdfminer로 텍스트를 뽑으면 보통
    1) 주소(동/지번/호/층) 블록이 쭉 나오고,
    2) 그 다음에 같은 순서로 가격(보증금/월세/관리비) 블록이 쭉 나오는
       형태로 직렬화되는 경우가 많다.

    그래서 이 버전에서는
      - 전체 라인에서 '주소 라인'만 순서대로 모으고,
      - 전체 라인에서 '순수 가격 라인(숫자/숫자(/숫자))'만 순서대로 모은 뒤,
      - 주소 리스트와 가격 리스트를 **위에서부터 차례대로 1:1 매칭**한다.

    라인 인덱스를 함께 가지고 있으므로, 같은 건물 안에서
    여러 호실이 섞여 있어도 PDF 상에 인쇄된 순서대로 매칭된다.

    가격 라인은 다음 조건을 만족하는 것만 사용한다.
      - "숫자/숫자" 또는 "숫자/숫자/숫자" 구조
      - 앞뒤로 공백만 허용, 다른 한글/영문 텍스트가 섞여 있으면 가격으로 보지 않음

    반환 예시:
    [
      {
        'dong': '신당동',
        'jibeon': '1831-4',
        'ho': '205',
        'deposit': 4000,
        'rent': 40,
        'maintenance_fee': None,
      },
      ...
    ]
    """
    if extract_text is None:
        raise RuntimeError("pdfminer.six 미설치: `pip install pdfminer.six` 필요")

    text = extract_text(BytesIO(pdf_bytes))
    raw_lines = list(text.splitlines())

    def _to_int(tok):
        if tok is None:
            return None
        s = str(tok).replace(",", "").strip()
        if s == "":
            return None
        try:
            return int(float(s))
        except Exception:
            return None

    # 주소: 예)
    #  - "신당동 1831-4 205 / 2F"
    #  - "신당동 1782-9 2층 / 2F"  (이 경우 ho는 "2"로 인식됨)
    p_addr = re.compile(
        r"(?P<dong>[가-힣]+동)\s+"
        r"(?P<jibeon>\d+(?:-\d+)?)\s+"
        r"(?P<ho>\d{1,4})"
        r"(?:\s*/\s*(?P<floor>\d+)F)?"
    )

    # "숫자/숫자" 또는 "숫자/숫자/숫자" 형태의 '순수 가격 라인'만 골라내는 정규식
    #   - 예: "4,000/40", "500/35 / 5"
    #   - 전체 라인이 이 패턴과 일치해야 가격으로 인정 (주택타입/평수/날짜 등 오인 방지)
    p_price_line = re.compile(r"^\s*\d[\d,]*\s*/\s*\d[\d,]*(?:\s*/\s*\d[\d,]*)?\s*$")
    # 토큰 단위 파싱용 (보증금/월세/관리비)
    p_price3 = re.compile(r"(?P<deposit>[0-9,]+)\s*/\s*(?P<rent>[0-9,]+)\s*/\s*(?P<mfee>[0-9,]+)")
    p_price2 = re.compile(r"(?P<deposit>[0-9,]+)\s*/\s*(?P<rent>[0-9,]+)\b")

    # 1) 주소 라인 전수 수집 (라인 인덱스와 함께)
    addr_rows = []
    for idx, ln in enumerate(raw_lines):
        m = p_addr.search(ln)
        if m:
            row = {"idx": idx}
            row.update(m.groupdict())
            addr_rows.append(row)

    # 2) 가격 라인 전수 수집 (라인 인덱스와 함께)
    price_rows = []
    for idx, ln in enumerate(raw_lines):
        if not p_price_line.match(ln):
            continue
        m3 = p_price3.search(ln)
        m2 = None if m3 else p_price2.search(ln)
        if not (m3 or m2):
            continue
        d = (m3 or m2).groupdict()
        row = {"idx": idx}
        row.update(d)
        price_rows.append(row)

    results = []
    p_idx = 0  # price_rows 포인터 (한 번 사용한 가격 라인은 재사용하지 않음)

    for a in addr_rows:
        assigned = None
        # 현재 주소 라인 이후에 처음 등장하는 가격 라인을 찾는다.
        while p_idx < len(price_rows) and price_rows[p_idx]["idx"] <= a["idx"]:
            p_idx += 1
        if p_idx < len(price_rows):
            assigned = price_rows[p_idx]
            p_idx += 1

        if assigned:
            deposit = _to_int(assigned.get("deposit"))
            rent = _to_int(assigned.get("rent"))
            mfee = _to_int(assigned.get("mfee"))
        else:
            deposit = rent = mfee = None

        results.append({
            "dong": a["dong"],
            "jibeon": a["jibeon"],
            "ho": a["ho"],  # DB 비교 시 숫자만 추출해 매칭하므로 문자열 그대로 저장
            "deposit": deposit,
            "rent": rent,
            "maintenance_fee": mfee,
        })

    # (동, 지번, 호) 기준 중복 제거
    uniq, seen = [], set()
    for r in results:
        key = (r["dong"], r["jibeon"], r["ho"])
        if key not in seen:
            seen.add(key)
            uniq.append(r)

    return uniq

@app.route("/reconcile", methods=["GET", "POST"])
@login_required
def reconcile_upload():
    if request.method == "POST":
        files = request.files.getlist("files")
        if not files:
            # fallback: 단일 name="file"로 올라오는 경우도 처리
            f_single = request.files.get("file")
            if f_single:
                files = [f_single]
        # 유효성 검사: PDF만 허용
        valid = [f for f in files if f and f.filename.lower().endswith('.pdf')]
        if not valid:
            flash("PDF 파일을 하나 이상 업로드하세요.", "warning")
            return redirect(url_for("reconcile_upload"))

        # 모든 PDF를 파싱하여 결과 합치기
        parsed_all = []
        try:
            for f in valid:
                pdf_bytes = f.read()
                parsed_all.extend(parse_pdf_for_units(pdf_bytes))
        except Exception as e:
            flash(f"파싱 실패: {e}", "danger")
            return redirect(url_for("reconcile_upload"))

        # (dong, jibeon, ho) 기준 중복 제거
        tmp_seen = set()
        parsed = []
        for x in parsed_all:
            k = (x['dong'], x['jibeon'], x['ho'])
            if k not in tmp_seen:
                tmp_seen.add(k)
                parsed.append(x)

        # 업로드된 PDF에 포함된 동 목록 (정규화: 마지막 '~동'만)
        def normalize_dong(text: str):
            """'대구 달서구 호산동' -> '호산동' 과 같이 마지막 '~동' 토큰만 남긴다."""
            if not text:
                return None
            parts = re.split(r"[\s,]+", str(text).strip())
            # 뒤에서부터 '~동'으로 끝나는 토큰을 찾는다.
            for tok in reversed(parts):
                if tok.endswith("동"):
                    return tok
            # 못 찾으면 원문 마지막 토큰 반환
            return parts[-1] if parts else None

        pdf_dongs = { normalize_dong(x['dong']) for x in parsed if normalize_dong(x['dong']) }

        # 지오코딩 캐시 + 재시도 래퍼
        geocode_cache = {}
        def geocode_to_dong_jibeon_cached(addr_text: str):
            """카카오 지오코딩 결과를 (동, 지번)으로 캐싱하여 반환한다."""
            if not addr_text:
                return (None, None)
            key = addr_text.strip()
            if key in geocode_cache:
                return geocode_cache[key]
            # 내부 실제 호출 함수 (원래 구현을 재사용)
            def _call_once(query):
                try:
                    kakao_key = os.getenv('KAKAO_REST_API_KEY') or app.config.get('KAKAO_REST_API_KEY')
                    if not kakao_key:
                        return (None, None)
                    from urllib.parse import quote_plus
                    import requests
                    url = f'https://dapi.kakao.com/v2/local/search/address.json?query={quote_plus(query)}'
                    headers = {'Authorization': f'KakaoAK {kakao_key}'}
                    res = requests.get(url, headers=headers, timeout=3)
                    if res.status_code != 200:
                        return (None, None)
                    data = res.json()
                    docs = data.get('documents') or []
                    if not docs:
                        return (None, None)
                    info = docs[0]
                    a = info.get('address') or {}
                    dong = a.get('region_3depth_name') or a.get('region_3depth_h_name')
                    main_no = a.get('main_address_no')
                    sub_no = a.get('sub_address_no')
                    if dong and main_no:
                        jibeon = str(main_no) + (f"-{sub_no}" if sub_no and str(sub_no) != '0' else "")
                        return (normalize_dong(dong), jibeon)
                    return (normalize_dong(dong), None)
                except Exception:
                    return (None, None)
            # 1차 호출
            d, j = _call_once(key)
            # 실패 시 1회 재시도
            if d is None and j is None:
                d, j = _call_once(key)
            geocode_cache[key] = (d, j)
            return geocode_cache[key]

        def normalize_ho_to_numeric(text):
            """'203호', '0203' 등에서 숫자만 추출하고 선행 0을 제거해 문자열로 반환."""
            if not text:
                return None
            m = re.search(r"(\d{1,4})", str(text))
            if not m:
                return None
            try:
                return str(int(m.group(1)))
            except Exception:
                return m.group(1)


        # === [PRICE-ONLY PASS] (도로명 주소 + 호수) → 실패 시 (동 + 지번 + 호수) 기준 가격 업데이트 (숨김/복원과 분리) ===
        import re as _re_price

        def _norm_int(x):
            if x is None:
                return None
            s = str(x).replace(",", "").strip()
            if not s:
                return None
            try:
                return int(s)
            except Exception:
                return None

        def _norm_ho(text: str):
            """'203호', '0203', '203', '3층 전체' 등에서 숫자만 뽑아 선행 0 제거 후 문자열로 반환."""
            if not text:
                return None
            m = _re_price.search(r"(\d{1,4})", str(text))
            if not m:
                return None
            try:
                return str(int(m.group(1)))
            except Exception:
                return m.group(1)

        # (1) PDF 측: (정규화된 동, 지번, 호수) 기준 가격 맵 생성 (기존 triple 방식 유지)
        pdf_by_triple = {}
        for r in parsed:
            dong_raw = r.get("dong") or ""
            j_raw = (r.get("jibeon") or "").strip()
            ho_raw = r.get("ho")

            d_norm = normalize_dong(dong_raw)
            ho_norm = _norm_ho(ho_raw)

            if not d_norm or not j_raw or not ho_norm:
                continue

            pdf_by_triple[(d_norm, j_raw, ho_norm)] = {
                "deposit": _norm_int(r.get("deposit")),
                "rent": _norm_int(r.get("rent")),
                "mfee": _norm_int(r.get("maintenance_fee")),
            }

        # (1-1) PDF 측: (도로명 주소 + 호수) 기준 가격 맵 생성
        #       - PDF에는 지번만 있으므로, "상위 행정구역 + 동 + 지번" → 카카오 지오코딩으로 도로명 주소를 얻어 키로 사용한다.
        road_geocode_cache = {}

        def geocode_to_roadaddr_cached(query: str):
            """카카오 지오코딩으로 '도로명 주소(road_address.address_name)'만 정규화하여 캐싱.
            road_address가 없으면 None을 반환하고, 지번 주소(address_name)로는 대체하지 않는다.
            """
            if not query:
                return None
            key = query.strip()
            if not key:
                return None
            if key in road_geocode_cache:
                return road_geocode_cache[key]
            try:
                kakao_key = os.getenv("KAKAO_REST_API_KEY") or app.config.get("KAKAO_REST_API_KEY")
                if not kakao_key:
                    road_geocode_cache[key] = None
                    return None
                from urllib.parse import quote_plus
                import requests

                url = f"https://dapi.kakao.com/v2/local/search/address.json?query={quote_plus(key)}"
                headers = {"Authorization": f"KakaoAK {kakao_key}"}
                res = requests.get(url, headers=headers, timeout=3)
                if res.status_code != 200:
                    road_geocode_cache[key] = None
                    return None
                data = res.json()
                docs = data.get("documents") or []
                if not docs:
                    road_geocode_cache[key] = None
                    return None
                info = docs[0]
                road = info.get("road_address") or {}
                addr_name = (road.get("address_name") or "").strip()
                if not addr_name:
                    # 도로명 주소가 아예 없는 경우에는 None 처리 (지번 주소로는 매칭하지 않음)
                    road_geocode_cache[key] = None
                    return None
                road_geocode_cache[key] = addr_name
                return addr_name
            except Exception:
                road_geocode_cache[key] = None
                return None

        # DB에 들어있는 도로명 주소에서 상위 행정구역(prefix) 추출 (예: "대구 달서구")
        base_region = None
        try:
            sample = Listing.query.filter(Listing.road_addr.isnot(None), Listing.road_addr != "").first()
            if sample and sample.road_addr:
                parts = str(sample.road_addr).split()
                if len(parts) >= 2:
                    base_region = " ".join(parts[:2])
        except Exception:
            base_region = None

        pdf_by_road_ho = {}
        for r in parsed:
            dong_raw = (r.get("dong") or "").strip()
            jibeon_raw = (r.get("jibeon") or "").strip()
            ho_raw = r.get("ho")
            ho_norm = _norm_ho(ho_raw)

            if not dong_raw or not jibeon_raw or not ho_norm:
                continue

            # "대구 달서구 신당동 506-2" 형태로 질의해서 도로명 주소 정규화
            if base_region:
                query_for_pdf = f"{base_region} {dong_raw} {jibeon_raw}"
            else:
                query_for_pdf = f"{dong_raw} {jibeon_raw}"

            road_addr_norm = geocode_to_roadaddr_cached(query_for_pdf)
            if road_addr_norm:
                print("PDF_GEOCODE_DEBUG:", {
                    "query": query_for_pdf,
                    "road_addr_norm": road_addr_norm,
                    "dong": dong_raw,
                    "jibeon": jibeon_raw,
                    "ho_norm": ho_norm,
                    "deposit": _norm_int(r.get("deposit")),
                    "rent": _norm_int(r.get("rent")),
                    "mfee": _norm_int(r.get("maintenance_fee")),
                })
            else:
                print("PDF_GEOCODE_FAIL:", query_for_pdf)
                continue

            pdf_by_road_ho[(road_addr_norm, ho_norm)] = {
                "deposit": _norm_int(r.get("deposit")),
                "rent": _norm_int(r.get("rent")),
                "mfee": _norm_int(r.get("maintenance_fee")),
            }

        # (2) DB 측: 우선 (도로명 주소 + 호수)로 가격 업데이트 시도, 실패 시 triple(동+지번+호) 보조
        db_all_for_price = Listing.query.all()
        changed_price = []
        any_addr_updated = False

        for l in db_all_for_price:
            ho_norm = _norm_ho(l.ho or "")
            if not ho_norm:
                continue

            # 지번 주소를 항상 함께 정규화해서 jibun_addr 필드에 저장 (지번 표기용)
            addr_for_triple = (l.road_addr or "").strip() or (l.dong or "").strip()
            db_dong_geo, db_jibeon_geo = geocode_to_dong_jibeon_cached(addr_for_triple)

            db_dong = normalize_dong(db_dong_geo or (l.dong or "").strip())
            db_jibeon = db_jibeon_geo

            # 지오코딩이 지번을 못 주면, 기존 jibun_addr에서 보조 추출
            if not db_jibeon and getattr(l, "jibun_addr", None):
                m_jb = _re_price.search(r"(\d+(?:-\d+)?)", (l.jibun_addr or ""))
                if m_jb:
                    db_jibeon = m_jb.group(1)

            # 정규화된 동/지번이 모두 있으면 jibun_addr 필드에 "동 지번" 형식으로 저장
            if db_dong and db_jibeon:
                try:
                    new_jibun_addr = f"{db_dong} {db_jibeon}"
                    if getattr(l, "jibun_addr", None) != new_jibun_addr:
                        l.jibun_addr = new_jibun_addr
                        any_addr_updated = True
                except Exception:
                    pass

            matched_vals = None

            # 2-1) 1차: DB에 이미 저장된 road_addr 문자열을 그대로 사용해서 (road_addr, ho) 기준 매칭
            addr_src = (l.road_addr or "").strip()
            db_road_norm = addr_src if addr_src else None

            if db_road_norm:
                matched_vals = pdf_by_road_ho.get((db_road_norm, ho_norm))

            # 2-2) 2차: 도로명 매칭 실패 시, 기존 triple(동+지번+호수) 매칭 시도
            if not matched_vals and db_dong and db_jibeon:
                matched_vals = pdf_by_triple.get((db_dong, db_jibeon, ho_norm))

            if not matched_vals:
                continue

            new_dep = matched_vals["deposit"]
            new_rent = matched_vals["rent"]
            new_mfee = matched_vals["mfee"]

            old_dep = l.deposit
            old_rent = l.rent
            old_mfee = getattr(l, "maintenance_fee", None)

            updated = False
            if new_dep is not None and new_dep != old_dep:
                l.deposit = new_dep
                updated = True
            if new_rent is not None and new_rent != old_rent:
                l.rent = new_rent
                updated = True
            # 관리비는 PDF에 값이 있을 때만 수정 (없으면 그대로 유지)
            if new_mfee is not None and new_mfee != old_mfee:
                try:
                    l.maintenance_fee = new_mfee
                    updated = True
                except Exception:
                    pass

            if updated:
                changed_price.append({
                    "id": l.id,
                    "title": l.title,
                    "dong": l.dong,
                    "ho": l.ho,
                    "addr": (l.jibun_addr or l.road_addr),
                    "old_deposit": old_dep,
                    "new_deposit": l.deposit,
                    "old_rent": old_rent,
                    "new_rent": l.rent,
                    "old_mfee": old_mfee,
                    "new_mfee": getattr(l, "maintenance_fee", None),
                })

        # 1차 패스에서 가격이 변경된 경우, 숨김/복원 로직과는 별도로 먼저 커밋을 시도한다.
        if changed_price:
            try:
                db.session.commit()
            except Exception:
                try:
                    db.session.rollback()
                except Exception:
                    pass
                flash("가격 변경 저장 실패(트랜잭션 롤백). 서버 로그 확인 필요.", "danger")
                return render_template(
                    "reconcile_result.html",
                    parsed_count=len(parsed),
                    stay_rows=[],
                    del_rows=[],
                    restored_rows=[],
                    changed_price=changed_price,
                )


        # 1) PDF 기반 키/맵 (dong, jibeon, ho) — 정확 일치만 유지로 간주 + 가격 정보 보유
        pdf_keys_triple = {(normalize_dong(x['dong']), x['jibeon'], str(int(str(x['ho']).strip())) if str(x['ho']).strip().isdigit() else x['ho']) for x in parsed}
        pdf_map = { (normalize_dong(x['dong']), x['jibeon'], str(int(str(x['ho']).strip())) if str(x['ho']).strip().isdigit() else x['ho']): x for x in parsed }

        # 2) 사이트 매물 전수 조회 후, 아래 기준에 따라 유지/삭제 결정
        db_list = Listing.query.all()
        stay_ids, del_ids = set(), set()
        restored_ids = set()

        for l in db_list:
            # --- 호수: 비교는 숫자만 사용 (DB에 '203호'로 저장되어도 PDF '203'과 매칭되도록) ---
            ho_num = normalize_ho_to_numeric(l.ho or '')

            # --- DB 주소에서 (dong, jibeon) 정규화 시도 ---
            db_dong_geo, db_jibeon_geo = geocode_to_dong_jibeon_cached((l.road_addr or '').strip() or (l.dong or '').strip())
            db_dong = normalize_dong(db_dong_geo or (l.dong or '').strip())
            db_jibeon = db_jibeon_geo

            # 지오코딩이 지번을 못 주면, DB의 지번주소 문자열에서 보조 추출 (예: '호산동 358-11 ...' → '358-11')
            if not db_jibeon:
                import re as _re2
                m_jb = _re2.search(r'(\d+(?:-\d+)?)', (l.jibun_addr or ''))
                if m_jb:
                    db_jibeon = m_jb.group(1)

            # 이 배치에서 비교/정리할 동만 한정 (PDF에 나온 동들)
            if pdf_dongs:
                if not db_dong:
                    # 동 정보가 없으면 비교에서 제외 (표에도 안 나옴)
                    continue
                # 정규화된 동으로 정확 비교
                if db_dong not in pdf_dongs:
                    # 다른 동은 완전히 제외
                    continue

            matched = False
            if db_dong and db_jibeon and ho_num:
                if (db_dong, db_jibeon, ho_num) in pdf_keys_triple:
                    matched = True

            if matched:
                stay_ids.add(l.id)
                # 이전에 숨김 상태였다면 이번 동기화로 복원 대상에 추가
                try:
                    if getattr(l, 'hidden', False):
                        restored_ids.add(l.id)
                except Exception:
                    pass
            else:
                del_ids.add(l.id)

        # (두 번째 패스) 가격 변경은 1차 패스에서만 수행됨. (여기서는 가격 변경 없음)

        # --- 커밋 분리: 1) 가격 변경 먼저 저장, 2) 숨김 해제는 별도 트랜잭션 ---
        # 1) 가격 변경 커밋 (이 단계에서 숨김 필드를 건드리지 않는다)
        try:
            need_commit = bool(changed_price or any_addr_updated)
            if need_commit:  # 가격 또는 지번 주소 중 하나라도 변경된 항목이 있으면 커밋
                db.session.commit()
        except Exception as e:
            # 가격 커밋이 실패하면 이후 UI만 보여주고 종료 (숨김 해제는 시도하지 않음)
            try:
                db.session.rollback()
            except Exception:
                pass
            flash("가격 변경 사항 저장에 실패했습니다. 서버 로그를 확인해주세요.", "danger")
            return render_template(
                "reconcile_result.html",
                parsed_count=len(parsed),
                stay_rows=[],
                del_rows=[],
                restored_rows=[],
                changed_price=changed_price,
            )

        # 2) 숨김 해제(복원)만 별도로 시도하고 커밋
        restored_ids = set()
        try:
            for l in db_list:
                if l.id in stay_ids:
                    try:
                        if getattr(l, 'hidden', False):
                            l.hidden = False
                            restored_ids.add(l.id)
                    except Exception:
                        # hidden 컬럼이 없으면 조용히 패스
                        pass
            db.session.commit()
        except Exception:
            try:
                db.session.rollback()
            except Exception:
                pass
            # 숨김 해제 실패해도 가격은 이미 반영되어 있음
            flash("일부 매물의 숨김 해제 저장에 실패했습니다. (가격은 반영됨)", "warning")

        def rows_for_ids(idset):
            rows = []
            if not idset:
                return rows
            for li in Listing.query.filter(Listing.id.in_(list(idset))).all():
                rows.append({
                    "id": li.id,
                    "title": li.title,
                    "dong": li.dong,
                    "ho": li.ho,
                    "addr": (li.jibun_addr or li.road_addr),
                    "trade_type": li.trade_type,
                    "category": li.category,
                    "price": li.sale_price or li.deposit or 0
                })
            return rows

        restored_rows = rows_for_ids(restored_ids)
        stay_rows = rows_for_ids(stay_ids)
        del_rows  = rows_for_ids(del_ids)

        return render_template(
            "reconcile_result.html",
            parsed_count=len(parsed),
            stay_rows=stay_rows,
            del_rows=del_rows,
            restored_rows=restored_rows,
            changed_price=changed_price,
        )

    # GET: 업로드 폼
    return render_template("reconcile_upload.html")

@app.route("/reconcile/delete", methods=["POST"])
@login_required
def reconcile_delete():
    ids = request.form.getlist("delete_ids")
    if not ids:
        flash("숨김 처리할 매물을 선택하세요.", "warning")
        return redirect(url_for("reconcile_upload"))
    cnt = 0
    for sid in ids:
        l = db.session.get(Listing, int(sid))
        if l:
            try:
                l.hidden = True
                cnt += 1
            except Exception:
                # hidden 컬럼이 없는 경우에는 무시
                pass
    try:
        db.session.commit()
    except Exception:
        pass
    flash(f"{cnt}개의 매물을 숨김 처리했습니다.", "info")
    return redirect(url_for("listings"))

# 승인/반려 (관리자)
@app.route("/admin/approve/<int:listing_id>", methods=["POST"])
@login_required
def approve(listing_id):
    if not require_admin():
        flash("관리자만 가능합니다.", "danger")
        return redirect(url_for("listings"))
    l = db.session.get(Listing, listing_id)
    if l:
        l.status = "approved"
        try:
            l.hidden = False
        except Exception:
            pass
        db.session.commit()
        flash("승인되었습니다.", "success")
    return redirect(url_for("listing_detail", listing_id=listing_id))

@app.route("/admin/reject/<int:listing_id>", methods=["POST"])
@login_required
def reject(listing_id):
    if not require_admin():
        flash("관리자만 가능합니다.", "danger")
        return redirect(url_for("listings"))
    l = db.session.get(Listing, listing_id)
    if l:
        l.status = "rejected"
        db.session.commit()
        flash("반려되었습니다.", "warning")
    return redirect(url_for("listing_detail", listing_id=listing_id))

# 일괄 승인 (관리자)
@app.route("/admin/approve_bulk", methods=["POST"])
@login_required
def approve_bulk():
    if not require_admin():
        return jsonify({"ok": False, "error": "forbidden"}), 403
    # ids[] 또는 ids 형태 모두 허용
    ids = request.form.getlist("ids[]") or request.form.getlist("ids")
    updated = 0
    if not ids:
        return jsonify({"ok": True, "updated": 0})
    for sid in ids:
        try:
            l = db.session.get(Listing, int(sid))
        except Exception:
            l = None
        if l and l.status != "approved":
            l.status = "approved"
            try:
                l.hidden = False  # 승인 시 숨김 해제
            except Exception:
                pass
            updated += 1
    db.session.commit()
    return jsonify({"ok": True, "updated": updated})

# 이미지 제공
@app.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)


# 개별 이미지 JPG 다운로드/목록 제공 라우트
@app.route("/listings/<int:listing_id>/download_images")
@login_required
def download_all_images(listing_id):
    """
    기존: 이미지를 ZIP으로 묶어서 내려줌
    변경: 각 이미지를 JPG 형식으로 개별 다운로드할 수 있는 링크 목록 페이지를 보여줌.
         - 이미지가 1장만 있으면 바로 JPG로 변환하여 파일 다운로드 응답
         - 여러 장이면: 각 이미지를 JPG로 변환해서 받을 수 있는 링크 리스트를 HTML로 렌더링
    """
    l = db.session.get(Listing, listing_id)
    if not l:
        flash("존재하지 않는 매물입니다.", "warning")
        return redirect(url_for("listings"))

    imgs = list(getattr(l, "images", []) or [])
    if not imgs:
        flash("다운로드할 이미지가 없습니다.", "info")
        return redirect(url_for("listing_detail", listing_id=listing_id))

    # 이미지가 1장뿐이라면 바로 JPG로 변환해서 단일 파일 다운로드로 응답
    if len(imgs) == 1:
        img = imgs[0]
        return _send_single_image_as_jpg(l, img)

    # 여러 장인 경우: 각 이미지를 JPG로 변환해서 받을 수 있는 링크 목록 HTML로 렌더링
    # 별도 템플릿을 만들지 않고 간단한 HTML을 render_template_string으로 반환
    links_html = """
    <!doctype html>
    <html lang="ko">
    <head>
        <meta charset="utf-8">
        <title>이미지 다운로드 - {{ title }}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; padding: 16px; margin: 0; }
            h1 { font-size: 18px; margin-bottom: 12px; }
            p { font-size: 13px; color: #555; margin-bottom: 12px; }
            ul { list-style: none; padding: 0; margin: 0; }
            li { margin-bottom: 8px; }
            a.download-link {
                display: inline-block;
                padding: 8px 12px;
                border-radius: 6px;
                border: 1px solid #ddd;
                text-decoration: none;
                font-size: 13px;
            }
            a.download-link:hover {
                background: #f5f5f5;
            }
        </style>
    </head>
    <body>
        <h1>이미지 개별 다운로드 (JPG)</h1>
        <p>아래 링크를 하나씩 눌러서 사진을 JPG 파일로 저장할 수 있습니다.</p>
        <ul>
            {% for img in imgs %}
            <li>
                <a class="download-link" href="{{ url_for('download_image_jpg', listing_id=listing_id, image_id=img.id) }}">
                    이미지 {{ loop.index }} JPG 저장
                </a>
            </li>
            {% endfor %}
        </ul>
        <script>
        window.addEventListener('load', function () {
            const links = document.querySelectorAll('a.download-link');
            if (!links.length) return;

            const doBulk = confirm('이 매물의 모든 이미지를 한 번에 다운로드할까요?');
            if (!doBulk) return;

            let idx = 0;
            const delay = 500; // ms, 너무 빠르면 브라우저가 막을 수 있어 약간 딜레이

            function triggerNext() {
                if (idx >= links.length) return;
                const a = document.createElement('a');
                a.href = links[idx].href;
                a.download = '';
                a.style.display = 'none';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                idx += 1;
                setTimeout(triggerNext, delay);
            }

            triggerNext();
        });
        </script>
    </body>
    </html>
    """
    return render_template_string(
        links_html,
        title=l.title or f"매물 {l.id}",
        imgs=imgs,
        listing_id=listing_id,
    )


# 단일 이미지 JPG 변환 및 다운로드 헬퍼 & 라우트
def _send_single_image_as_jpg(listing, img_obj):
    """
    단일 ListingImage를 JPG로 변환하여 다운로드 응답으로 보내는 헬퍼.
    원본이 webp/기타 형식이어도 무조건 JPEG로 변환해서 내려준다.
    """
    filename = getattr(img_obj, "filename", None)
    if not filename:
        flash("이미지 파일을 찾을 수 없습니다.", "warning")
        return redirect(url_for("listing_detail", listing_id=listing.id))

    img_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    if not os.path.isfile(img_path):
        flash("이미지 파일이 존재하지 않습니다.", "warning")
        return redirect(url_for("listing_detail", listing_id=listing.id))

    # Pillow가 없는 경우: 원본 파일을 그대로 내려보내되, 확장자를 .jpg로 바꿔서 제공
    # (실제 포맷은 그대로이지만, 환경 제약 시의 최소 fallback 용도)
    if PIL_Image is None:
        from werkzeug.utils import secure_filename as _sec
        base_name, _dot, _ext = filename.rpartition(".")
        if not base_name:
            base_name = f"listing-{listing.id}-{img_obj.id}"
        download_name = _sec(f"{base_name}.jpg")
        return send_file(
            img_path,
            mimetype="image/jpeg",
            as_attachment=True,
            download_name=download_name,
        )

    # Pillow가 있는 경우: 항상 실제 JPEG로 변환하여 메모리 버퍼에 저장 후 전송
    from io import BytesIO
    buf = BytesIO()
    try:
        img = PIL_Image.open(img_path)
        # EXIF 방향 교정
        try:
            if 'PIL_ImageOps' in globals() and PIL_ImageOps is not None:
                img = PIL_ImageOps.exif_transpose(img)
        except Exception:
            pass

        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGB")
        elif img.mode == "RGBA":
            # 투명 배경이 있다면 흰색 배경으로 합성
            bg = PIL_Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[-1])
            img = bg

        img.save(buf, format="JPEG", quality=90, optimize=True, progressive=True)
    except Exception:
        # 변환 실패 시 원본 파일 그대로 전달 (단, 이름은 .jpg로)
        buf = None

    from werkzeug.utils import secure_filename as _sec
    base_name, _dot, _ext = filename.rpartition(".")
    if not base_name:
        base_name = f"listing-{listing.id}-{img_obj.id}"
    download_name = _sec(f"{base_name}.jpg")

    if buf is None:
        # 변환 실패 fallback
        return send_file(
            img_path,
            mimetype="image/jpeg",
            as_attachment=True,
            download_name=download_name,
        )

    buf.seek(0)
    return send_file(
        buf,
        mimetype="image/jpeg",
        as_attachment=True,
        download_name=download_name,
    )


@app.route("/listings/<int:listing_id>/images/<int:image_id>/jpg")
@login_required
def download_image_jpg(listing_id, image_id):
    """
    개별 이미지 1장을 JPG로 변환해서 바로 다운로드하는 라우트.
    상세 페이지의 '이미지 다운로드' 버튼 또는
    /listings/<id>/download_images에서 생성한 링크들이 이 엔드포인트를 사용한다.
    """
    l = db.session.get(Listing, listing_id)
    if not l:
        flash("존재하지 않는 매물입니다.", "warning")
        return redirect(url_for("listings"))

    target = None
    for img in getattr(l, "images", []) or []:
        if img.id == image_id:
            target = img
            break
    if not target:
        flash("해당 이미지를 찾을 수 없습니다.", "warning")
        return redirect(url_for("listing_detail", listing_id=listing_id))

    return _send_single_image_as_jpg(l, target)

# 간단 API (현 뷰 필터와 동일 파라미터를 재사용해 JSON 응답)
@app.route("/api/listings")
@login_required
def api_listings():
    # 재활용 위해 listings()의 필터 로직을 간략화해 반영
    q = Listing.query.filter(Listing.status == "approved")
    try:
        q = q.filter(or_(Listing.hidden.is_(False), Listing.hidden.is_(None)))
    except Exception:
        pass
    region = (request.args.get("region") or request.args.get("q") or "").strip()
    if region:
        like = f"%{region}%"
        q = q.filter(or_(Listing.road_addr.ilike(like), Listing.jibun_addr.ilike(like), Listing.dong.ilike(like)))

    selected_dongs = request.args.getlist("dongs")
    if selected_dongs:
        q = q.filter(Listing.dong.in_(selected_dongs))

    only_gyeongsan = (request.args.get("only_gyeongsan") or "0").strip()
    if only_gyeongsan == "1":
        try:
            q = q.filter(
                or_(
                    Listing.road_addr.ilike("%경산%"),
                    Listing.jibun_addr.ilike("%경산%"),
                )
            )
        except Exception:
            try:
                q = q.filter(Listing.road_addr.ilike("%경산%"))
            except Exception:
                pass

    items = q.order_by(Listing.created_at.desc()).limit(200).all()
    return jsonify([{
        "id": x.id,
        "title": x.title,
        "lat": x.lat, "lng": x.lng,
        "addr": (x.jibun_addr or x.road_addr),
        "category": x.category,
        "trade_type": x.trade_type,
        "price": x.sale_price or x.deposit or 0
    } for x in items])

 # 이미지 순서 재정렬 API (정상 등록 위치)
@app.route("/listings/<int:listing_id>/images/reorder", methods=["POST"])
@login_required
def reorder_images(listing_id):
    l = db.session.get(Listing, listing_id)
    if not l:
        return jsonify({"ok": False, "error": "not_found"}), 404
    payload = request.get_json(silent=True) or {}
    order = payload.get("order")
    if not isinstance(order, list):
        return jsonify({"ok": False, "error": "bad_request"}), 400
    # Map images by id for this listing only
    imgs = {img.id: img for img in l.images}
    for idx, img_id in enumerate(order):
        try:
            img_id_int = int(img_id)
        except (TypeError, ValueError):
            continue
        img = imgs.get(img_id_int)
        if img:
            img.sort_order = idx
    db.session.commit()
    return jsonify({"ok": True})

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        # 기본 관리자 계정(없으면)
        if not User.query.filter_by(email="sindang1234").first():
            admin = User(email="sindang1234", name="관리자", role="admin")
            admin.set_password("1234")
            db.session.add(admin)
            db.session.commit()
            print("✓ 기본 관리자(sindang1234 / 1234) 생성")
        # 기본 직원 계정(없으면)
        if not User.query.filter_by(email="sd1234").first():
            u = User(email="sd1234", name="직원", role="staff")
            u.set_password("1234")
            db.session.add(u)
            db.session.commit()
            print("✓ 기본 직원(sd1234 / 1234) 생성")
    app.run(host="0.0.0.0", port=25565, debug=True)