"""
Fraud Detection Dashboard
Displays: (1) Duplicate records, (2) Credit Note / Invoice ratios
Data source: CSV export from Athena GENERATE_IRN query

Run: python app.py
Open: http://localhost:5000
"""

import csv
import json
import os
from collections import defaultdict
from flask import Flask, render_template_string, jsonify, request as flask_request, send_file

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CSV_PATH = os.path.join(DATA_DIR, "duplicates.csv")
CRN_RATIO_PATH = os.path.join(DATA_DIR, "crn_ratio.json")
FULL_CSV = os.path.join(DATA_DIR, "1 Month Data.csv")

KEY_COLS = ["BuyerDtls_Gstin", "SellerDtls_Gstin", "DocDtls_Dt", "DocDtls_No"]

DISPLAY_COLS_PREFERRED = [
    "DocDtls_No", "DocDtls_Dt", "DocDtls_Typ",
    "SellerDtls_Gstin", "SellerDtls_LglNm",
    "BuyerDtls_Gstin", "BuyerDtls_LglNm",
    "TranDtls_SupTyp", "ValDtls_TotInvVal", "ValDtls_AssVal",
    "ValDtls_CgstVal", "ValDtls_SgstVal", "ValDtls_IgstVal",
    "ItemList_HsnCd", "ItemList_PrdDesc", "ItemList_Qty",
    "ItemList_UnitPrice", "ItemList_TotItemVal",
    "CustomFields_ErpSource"
]

_cache = {}


def load_duplicates():
    if "dup_rows" in _cache:
        return _cache["dup_rows"], _cache["dup_display_cols"], _cache["dup_stats"]

    if not os.path.exists(CSV_PATH):
        return [], [], {}

    rows = []
    try:
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames or []
            for row in reader:
                rows.append(row)
    except Exception:
        return [], [], {}

    display_cols = [c for c in DISPLAY_COLS_PREFERRED if c in columns]

    groups = defaultdict(list)
    for i, row in enumerate(rows):
        vals = [(row.get(k) or "").strip() for k in KEY_COLS]
        if all(vals):
            key = tuple(vals)
            groups[key].append(i)

    dup_rows = []
    group_id = 0
    total_value = 0.0
    unique_sellers = set()

    for key in sorted(groups.keys()):
        indices = groups[key]
        if len(indices) <= 1:
            continue
        for idx in indices:
            row = rows[idx]
            row["_group_id"] = group_id
            row["_group_size"] = len(indices)
            dup_rows.append(row)
            try:
                total_value += float(row.get("ValDtls_TotInvVal") or 0)
            except (ValueError, TypeError):
                pass
            s = (row.get("SellerDtls_Gstin") or "").strip()
            if s:
                unique_sellers.add(s)
        group_id += 1

    stats = {
        "total_rows": len(dup_rows),
        "num_groups": group_id,
        "total_value": total_value,
        "unique_sellers": len(unique_sellers),
    }

    _cache["dup_rows"] = dup_rows
    _cache["dup_display_cols"] = display_cols
    _cache["dup_stats"] = stats
    return dup_rows, display_cols, stats


def load_crn_ratios():
    if "crn_data" in _cache:
        return _cache["crn_data"], _cache["crn_stats"]

    if not os.path.exists(CRN_RATIO_PATH):
        return [], {}

    try:
        with open(CRN_RATIO_PATH, "r") as f:
            data = json.load(f)
    except Exception:
        return [], {}

    stats = {
        "total_sellers": len(data),
        "high_ratio": sum(1 for x in data if x.get("crn_inv_ratio", 0) > 0.5),
        "extreme_ratio": sum(1 for x in data if x.get("crn_inv_ratio", 0) > 1.0),
        "total_crn_val": sum(x.get("total_crn_val", 0) for x in data),
    }

    _cache["crn_data"] = data
    _cache["crn_stats"] = stats
    return data, stats


# ─── HTML TEMPLATE ────────────────────────────────────────────────────────────

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Fraud Detection Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css" rel="stylesheet">
    <style>
        :root { --dark: #1a1a2e; --accent: #e94560; --blue: #0f3460; }
        body { background: #f0f2f5; font-size: 13px; }
        .header-bar {
            background: linear-gradient(135deg, var(--dark) 0%, var(--blue) 100%);
            color: white; padding: 18px 0; margin-bottom: 24px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        .header-bar h1 { font-size: 1.4rem; margin: 0; font-weight: 600; letter-spacing: 0.5px; }
        .header-bar .subtitle { opacity: 0.7; font-size: 0.8rem; margin-top: 2px; }

        /* Nav tabs */
        .nav-tabs .nav-link { color: #555; font-weight: 500; border: none; padding: 10px 20px; }
        .nav-tabs .nav-link.active { color: var(--accent); border-bottom: 3px solid var(--accent); background: transparent; font-weight: 700; }
        .nav-tabs { border-bottom: 2px solid #dee2e6; margin-bottom: 20px; }

        /* Stat cards */
        .stat-card {
            background: white; border-radius: 10px; padding: 16px 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06); text-align: center;
            border-left: 4px solid var(--blue);
        }
        .stat-card.danger { border-left-color: var(--accent); }
        .stat-card.warning { border-left-color: #ffc107; }
        .stat-card.success { border-left-color: #198754; }
        .stat-card .number { font-size: 1.6rem; font-weight: 700; color: var(--dark); }
        .stat-card .label { font-size: 0.75rem; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }

        /* Table styling */
        .table-container {
            background: white; border-radius: 10px; padding: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        }
        .group-even { background-color: #ffffff !important; }
        .group-odd { background-color: #fff8e1 !important; }
        .group-even:hover { background-color: #e3f2fd !important; }
        .group-odd:hover { background-color: #fff3cd !important; }
        .key-col { font-weight: 600; color: #0d6efd; }
        .val-col { font-family: 'SF Mono', monospace; text-align: right; }
        table.dataTable { font-size: 12px; }
        table.dataTable th { white-space: nowrap; font-size: 11px; background: var(--dark); color: white; }
        table.dataTable td { max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        table.dataTable td:hover { overflow: visible; white-space: normal; word-break: break-all; }

        /* Risk badges */
        .risk-extreme { background: #dc3545; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
        .risk-high { background: #fd7e14; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
        .risk-medium { background: #ffc107; color: #333; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
        .risk-low { background: #198754; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }

        .badge-dup { font-size: 0.7rem; }
        .loading-overlay {
            position: fixed; top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(255,255,255,0.85); display: flex;
            align-items: center; justify-content: center; z-index: 9999;
        }
        .loading-overlay .spinner-border { width: 3rem; height: 3rem; }
        .dataTables_wrapper .dataTables_filter input { width: 300px; }
        .tab-pane { min-height: 400px; }
    </style>
</head>
<body>
    <div class="header-bar">
        <div class="container-fluid px-4">
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <h1>Fraud Detection Dashboard</h1>
                    <div class="subtitle">E-Invoice Audit Trail Analysis &mdash; Feb 2026</div>
                </div>
                <div>
                    {% if has_dup_data %}
                    <a href="/export/duplicates" class="btn btn-outline-light btn-sm">Export Duplicates</a>
                    {% endif %}
                    {% if has_crn_data %}
                    <a href="/export/crn-ratio" class="btn btn-outline-light btn-sm ms-1">Export CRN Ratios</a>
                    {% endif %}
                </div>
            </div>
        </div>
    </div>

    <div class="container-fluid px-4 mb-4">
        <!-- Tabs -->
        <ul class="nav nav-tabs" id="mainTabs" role="tablist">
            <li class="nav-item">
                <button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tabDuplicates" type="button">
                    Duplicate Records
                    {% if has_dup_data %}<span class="badge bg-danger ms-1">{{ "{:,}".format(dup_stats.num_groups) }}</span>{% endif %}
                </button>
            </li>
            <li class="nav-item">
                <button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabCrnRatio" type="button">
                    Credit Note / Invoice Ratio
                    {% if has_crn_data %}<span class="badge bg-warning text-dark ms-1">{{ "{:,}".format(crn_stats.extreme_ratio) }}</span>{% endif %}
                </button>
            </li>
        </ul>

        <div class="tab-content">
            <!-- ═══ TAB 1: DUPLICATES ═══ -->
            <div class="tab-pane fade show active" id="tabDuplicates">
                {% if has_dup_data %}
                <div class="row g-3 mb-4">
                    <div class="col-md-3">
                        <div class="stat-card danger">
                            <div class="number">{{ "{:,}".format(dup_stats.total_rows) }}</div>
                            <div class="label">Total Duplicate Rows</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="stat-card warning">
                            <div class="number">{{ "{:,}".format(dup_stats.num_groups) }}</div>
                            <div class="label">Duplicate Groups</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="stat-card">
                            <div class="number">{{ "{:,.0f}".format(dup_stats.total_value) }}</div>
                            <div class="label">Total Invoice Value (&#8377;)</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="stat-card success">
                            <div class="number">{{ "{:,}".format(dup_stats.unique_sellers) }}</div>
                            <div class="label">Unique Sellers Involved</div>
                        </div>
                    </div>
                </div>
                <div class="table-container">
                    <div id="dupLoading" class="loading-overlay">
                        <div class="text-center">
                            <div class="spinner-border text-primary"></div>
                            <div class="mt-2">Loading duplicate records...</div>
                        </div>
                    </div>
                    <table id="dupTable" class="table table-sm table-bordered" style="width:100%">
                        <thead>
                            <tr>
                                <th>Group</th>
                                <th>#</th>
                                {% for col in display_cols %}
                                <th>{{ col }}</th>
                                {% endfor %}
                            </tr>
                        </thead>
                    </table>
                </div>
                {% else %}
                <div class="table-container" style="text-align:center;padding:60px;color:#666;">
                    <h4>No Duplicate Data</h4>
                    <p>Place duplicate CSV at: <code>{{ csv_path }}</code></p>
                </div>
                {% endif %}
            </div>

            <!-- ═══ TAB 2: CRN / INV RATIO ═══ -->
            <div class="tab-pane fade" id="tabCrnRatio">
                {% if has_crn_data %}
                <div class="row g-3 mb-4">
                    <div class="col-md-3">
                        <div class="stat-card">
                            <div class="number">{{ "{:,}".format(crn_stats.total_sellers) }}</div>
                            <div class="label">Sellers Analyzed</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="stat-card danger">
                            <div class="number">{{ "{:,}".format(crn_stats.extreme_ratio) }}</div>
                            <div class="label">CRN/INV Ratio &gt; 1.0</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="stat-card warning">
                            <div class="number">{{ "{:,}".format(crn_stats.high_ratio) }}</div>
                            <div class="label">CRN/INV Ratio &gt; 0.5</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="stat-card">
                            <div class="number">{{ "{:,.0f}".format(crn_stats.total_crn_val) }}</div>
                            <div class="label">Total Credit Note Value (&#8377;)</div>
                        </div>
                    </div>
                </div>
                <div class="table-container">
                    <div id="crnLoading" class="loading-overlay" style="display:none;">
                        <div class="text-center">
                            <div class="spinner-border text-warning"></div>
                            <div class="mt-2">Loading CRN ratio data...</div>
                        </div>
                    </div>
                    <table id="crnTable" class="table table-sm table-bordered" style="width:100%">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>Risk</th>
                                <th>Seller GSTIN</th>
                                <th>Seller Name</th>
                                <th>CRN/INV Ratio</th>
                                <th>Invoices (INV)</th>
                                <th>Credit Notes (CRN)</th>
                                <th>Debit Notes (DBN)</th>
                                <th>Total Invoice Value (&#8377;)</th>
                                <th>Total CRN Value (&#8377;)</th>
                            </tr>
                        </thead>
                    </table>
                </div>
                {% else %}
                <div class="table-container" style="text-align:center;padding:60px;color:#666;">
                    <h4>No CRN Ratio Data</h4>
                    <p>Run the data preparation script first.</p>
                </div>
                {% endif %}
            </div>
        </div>
    </div>

    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
    <script>
    function riskBadge(ratio) {
        if (ratio > 1.0) return '<span class="risk-extreme">EXTREME</span>';
        if (ratio > 0.5) return '<span class="risk-high">HIGH</span>';
        if (ratio > 0.2) return '<span class="risk-medium">MEDIUM</span>';
        return '<span class="risk-low">LOW</span>';
    }
    function fmtNum(n) {
        if (n == null || n === '-') return '-';
        return Number(n).toLocaleString('en-IN', {maximumFractionDigits: 2});
    }

    $(document).ready(function() {
        {% if has_dup_data %}
        $('#dupTable').DataTable({
            processing: true,
            serverSide: true,
            ajax: { url: '/api/duplicates', type: 'GET' },
            columns: [
                { data: '_group_id', render: function(d, t, row) {
                    var cls = (d % 2 === 0) ? 'bg-primary' : 'bg-warning text-dark';
                    return '<span class="badge ' + cls + ' badge-dup">G' + (d+1) + '</span>' +
                           ' <small class="text-muted">(' + row._group_size + ')</small>';
                }},
                { data: '_row_num', className: 'text-muted' },
                {% for col in display_cols %}
                { data: '{{ col }}', defaultContent: '-',
                  className: '{{ "key-col" if col in key_cols else "" }}{{ " val-col" if "Val" in col or "Price" in col or "Amt" in col else "" }}'
                  {% if "Val" in col or "Price" in col or "Amt" in col %}
                  , render: function(d) { return fmtNum(d); }
                  {% endif %}
                },
                {% endfor %}
            ],
            pageLength: 100,
            order: [[0, 'asc']],
            scrollX: true,
            deferRender: true,
            createdRow: function(row, data) {
                $(row).addClass(data._group_id % 2 === 0 ? 'group-even' : 'group-odd');
            },
            language: { search: "Search:", info: "Showing _START_-_END_ of _TOTAL_ duplicate rows" },
            initComplete: function() { $('#dupLoading').fadeOut(200); }
        });
        {% endif %}

        {% if has_crn_data %}
        var crnLoaded = false;
        $('button[data-bs-target="#tabCrnRatio"]').on('shown.bs.tab', function() {
            if (crnLoaded) return;
            crnLoaded = true;
            $('#crnLoading').show();
            $('#crnTable').DataTable({
                processing: true,
                serverSide: true,
                ajax: { url: '/api/crn-ratio', type: 'GET' },
                columns: [
                    { data: '_row_num', className: 'text-muted' },
                    { data: 'crn_inv_ratio', render: function(d) { return riskBadge(d); }, orderable: false },
                    { data: 'gstin', className: 'key-col' },
                    { data: 'name' },
                    { data: 'crn_inv_ratio', className: 'val-col', render: function(d) { return d.toFixed(4); } },
                    { data: 'inv_count', className: 'val-col', render: function(d) { return fmtNum(d); } },
                    { data: 'crn_count', className: 'val-col', render: function(d) { return fmtNum(d); } },
                    { data: 'dbn_count', className: 'val-col', render: function(d) { return fmtNum(d); } },
                    { data: 'total_inv_val', className: 'val-col', render: function(d) { return fmtNum(d); } },
                    { data: 'total_crn_val', className: 'val-col', render: function(d) { return fmtNum(d); } },
                ],
                pageLength: 50,
                order: [[4, 'desc']],
                scrollX: true,
                language: { search: "Search Seller:", info: "Showing _START_-_END_ of _TOTAL_ sellers" },
                initComplete: function() { $('#crnLoading').fadeOut(200); }
            });
        });
        {% endif %}
    });
    </script>
</body>
</html>
"""


# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    dup_rows, display_cols, dup_stats = load_duplicates()
    crn_data, crn_stats = load_crn_ratios()

    return render_template_string(
        HTML_TEMPLATE,
        display_cols=display_cols,
        key_cols=KEY_COLS,
        dup_stats=dup_stats if dup_rows else {},
        has_dup_data=len(dup_rows) > 0,
        crn_stats=crn_stats if crn_data else {},
        has_crn_data=len(crn_data) > 0,
        csv_path=CSV_PATH,
    )


@app.route("/api/duplicates")
def api_duplicates():
    rows, _, _ = load_duplicates()

    draw = int(flask_request.args.get("draw", 1))
    start = int(flask_request.args.get("start", 0))
    length = int(flask_request.args.get("length", 100))
    search = flask_request.args.get("search[value]", "").strip().lower()

    if search:
        filtered = [r for r in rows if any(search in str(v).lower() for v in r.values() if v)]
    else:
        filtered = rows

    page = filtered[start:start + length]
    result = []
    for i, row in enumerate(page):
        r = {k: (v if v else "-") for k, v in row.items() if not k.startswith("_")}
        r["_group_id"] = row.get("_group_id", 0)
        r["_group_size"] = row.get("_group_size", 0)
        r["_row_num"] = start + i + 1
        result.append(r)

    return jsonify({"draw": draw, "recordsTotal": len(rows),
                     "recordsFiltered": len(filtered), "data": result})


@app.route("/api/crn-ratio")
def api_crn_ratio():
    data, _ = load_crn_ratios()

    draw = int(flask_request.args.get("draw", 1))
    start = int(flask_request.args.get("start", 0))
    length = int(flask_request.args.get("length", 50))
    search = flask_request.args.get("search[value]", "").strip().lower()

    # Sort
    order_col = int(flask_request.args.get("order[0][column]", 4))
    order_dir = flask_request.args.get("order[0][dir]", "desc")
    col_map = {0: "_row_num", 2: "gstin", 3: "name", 4: "crn_inv_ratio",
               5: "inv_count", 6: "crn_count", 7: "dbn_count",
               8: "total_inv_val", 9: "total_crn_val"}
    sort_key = col_map.get(order_col, "crn_inv_ratio")

    if search:
        filtered = [r for r in data if search in r.get("gstin", "").lower() or search in r.get("name", "").lower()]
    else:
        filtered = data

    if sort_key != "_row_num":
        filtered = sorted(filtered, key=lambda x: x.get(sort_key, 0),
                          reverse=(order_dir == "desc"))

    page = filtered[start:start + length]
    for i, row in enumerate(page):
        row["_row_num"] = start + i + 1

    return jsonify({"draw": draw, "recordsTotal": len(data),
                     "recordsFiltered": len(filtered), "data": page})


@app.route("/export/duplicates")
def export_duplicates():
    if not os.path.exists(CSV_PATH):
        return "No data", 404
    return send_file(CSV_PATH, mimetype="text/csv",
                     as_attachment=True, download_name="fraud_duplicates.csv")


@app.route("/export/crn-ratio")
def export_crn_ratio():
    data, _ = load_crn_ratios()
    if not data:
        return "No data", 404

    import io
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["gstin", "name", "crn_inv_ratio",
                                                  "inv_count", "crn_count", "dbn_count",
                                                  "total_inv_val", "total_crn_val"])
    writer.writeheader()
    writer.writerows(data)
    output.seek(0)

    from flask import Response
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=crn_inv_ratio.csv"})


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print("\nStarting Fraud Detection Dashboard at http://localhost:5000")
    app.run(host="127.0.0.1", port=5000, debug=True)
