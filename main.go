package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

// ---------- config ----------

type Config struct {
	Mode               string `yaml:"mode"`
	RecomputeBatchSize int    `yaml:"recompute_batch_size"`
	IsDebug            int    `yaml:"isdebug"` // 新增
	Database           struct {
		Development struct {
			Dialect string `yaml:"dialect"`
			DSN     string `yaml:"dsn"`
		} `yaml:"development"`
	} `yaml:"database"`
	Dirs struct {
		Logs string `yaml:"logs"`
	} `yaml:"dirs"`
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	if cfg.RecomputeBatchSize <= 0 {
		cfg.RecomputeBatchSize = 100 // 需求：預設 100
	}
	if cfg.Dirs.Logs == "" {
		cfg.Dirs.Logs = "."
	}
	_ = os.MkdirAll(cfg.Dirs.Logs, 0o755)
	cfg.Dirs.Logs = filepath.Join(cfg.Dirs.Logs, "log.txt")
	return cfg, nil
}

// ---------- logging ----------

func newLogger(logPath string) *log.Logger {
	lj := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    100,
		MaxAge:     7,
		MaxBackups: 7,
		Compress:   false,
	}
	return log.New(lj, "", log.LstdFlags)
}

// ---------- data structures ----------

type AmountFieldSet struct {
	Base string
	Usdt string
	Cny  string
}

type FieldMapping struct {
	BaseAmount string
	CnyAmount  string
	UsdtAmount string
	MainCode   string
	SubCode    string
	SiteCode   string
	IDColumn   string
	AmountSets []AmountFieldSet
}

type recordRow struct {
	ID        uint64
	Currency  sql.NullString
	EntryDate sql.NullTime
	SubCode   string
	SiteCode  string
	Amounts   map[string]sql.NullFloat64 // key: column name
}

type officeInfo struct {
	MainCode   string
	SubCode    string
	SiteCode   string
	MainOffice string
	SubOffice  string
	Site       string
}

// ---------- table mappings (同原本) ---------
/* 省略：保持原 TableFieldMappings 全表定義，與你現有一致即可 */

var TableFieldMappings = map[string]FieldMapping{
	"acc_cashbook": {
		MainCode: "main_office", SubCode: "sub_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			{Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"acc_expenses": {
		MainCode: "main_office", SubCode: "sub_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			{Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"acc_borrow_lend": {
		MainCode: "main_office", SubCode: "sub_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			{Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"acc_recharge_withdraw": {
		MainCode: "main_office", SubCode: "sub_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "recharge_amount", Usdt: "recharge_amount_usdt", Cny: "recharge_amount_cny"},
			{Base: "withdraw_amount", Usdt: "withdraw_amount_usdt", Cny: "withdraw_amount_cny"},
			{Base: "commission", Usdt: "commission_usdt", Cny: "commission_cny"},
			{Base: "discount", Usdt: "discount_usdt", Cny: "discount_cny"},
			{Base: "first_topup_amount", Usdt: "first_topup_amount_USDT", Cny: "first_topup_amount_CNY"},
			{Base: "manual_score_increase", Usdt: "manual_score_increase_usdt", Cny: "manual_score_increase_cny"},
			{Base: "manual_score_decrease", Usdt: "manual_score_decrease_usdt", Cny: "manual_score_decrease_cny"},
			{Base: "total_score_balance", Usdt: "total_score_balance_usdt", Cny: "total_score_balance_cny"},
		},
	},
	"acc_channel_info": {
		MainCode: "main_office", SubCode: "sub_office", SiteCode: "site_code", IDColumn: "id",
	},
	"acc_ad_performance_analysis": {
		MainCode: "main_office", SubCode: "sub_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "first_topup_amount", Usdt: "first_topup_amount_USDT", Cny: "first_topup_amount_CNY"},
			{Base: "repeat_topup_amount", Usdt: "repeat_topup_amount_USDT", Cny: "repeat_topup_amount_CNY"},
			{Base: "d2_topup_amount", Usdt: "d2_topup_amount_USDT", Cny: "d2_topup_amount_CNY"},
			{Base: "d3_topup_amount", Usdt: "d3_topup_amount_USDT", Cny: "d3_topup_amount_CNY"},
			{Base: "d4_topup_amount", Usdt: "d4_topup_amount_USDT", Cny: "d4_topup_amount_CNY"},
			{Base: "d5_topup_amount", Usdt: "d5_topup_amount_USDT", Cny: "d5_topup_amount_CNY"},
			{Base: "d6_topup_amount", Usdt: "d6_topup_amount_USDT", Cny: "d6_topup_amount_CNY"},
			{Base: "d7_topup_amount", Usdt: "d7_topup_amount_USDT", Cny: "d7_topup_amount_CNY"},
			{Base: "d14_topup_amount", Usdt: "d14_topup_amount_USDT", Cny: "d14_topup_amount_CNY"},
			{Base: "d15_topup_amount", Usdt: "d15_topup_amount_USDT", Cny: "d15_topup_amount_CNY"},
			{Base: "d30_topup_amount", Usdt: "d30_topup_amount_USDT", Cny: "d30_topup_amount_CNY"},
			{Base: "d45_topup_amount", Usdt: "d45_topup_amount_USDT", Cny: "d45_topup_amount_CNY"},
			{Base: "d60_topup_amount", Usdt: "d60_topup_amount_USDT", Cny: "d60_topup_amount_CNY"},
		},
	},
	"acc_balance_sheet": {
		MainCode: "main_office", SubCode: "sub_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "ending_amount", Usdt: "ending_amount_USDT", Cny: "ending_amount_CNY"},
			{Base: "income_amount", Usdt: "income_amount_USDT", Cny: "income_amount_CNY"},
			{Base: "non_member_income", Usdt: "non_member_income_USDT", Cny: "non_member_income_CNY"},
			{Base: "income_fee", Usdt: "income_fee_USDT", Cny: "income_fee_CNY"},
			{Base: "expense_amount", Usdt: "expense_amount_USDT", Cny: "expense_amount_CNY"},
			{Base: "non_member_expense", Usdt: "non_member_expense_USDT", Cny: "non_member_expense_CNY"},
			{Base: "expense_fee", Usdt: "expense_fee_USDT", Cny: "expense_fee_CNY"},
			{Base: "balance_difference", Usdt: "balance_difference_USDT", Cny: "balance_difference_CNY"},
			{Base: "opening_balance", Usdt: "opening_balance_USDT", Cny: "opening_balance_CNY"},
			{Base: "backend_revenue", Usdt: "backend_revenue_USDT", Cny: "backend_revenue_CNY"},
			{Base: "order_adjustment", Usdt: "order_adjustment_USDT", Cny: "order_adjustment_CNY"},
			{Base: "converted_amount", Usdt: "converted_amount_USDT", Cny: "converted_amount_CNY"},
			{Base: "balance_verification", Usdt: "balance_verification_USDT", Cny: "balance_verification_CNY"},
			{Base: "difference", Usdt: "difference_USDT", Cny: "difference_CNY"},
		},
	},
	"acc_revenue_expense_adjustments": {
		MainCode: "main_office", SubCode: "sub_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			{Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"acc_operational_information": {
		MainCode: "main_office", SubCode: "sub_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "valid_bet", Usdt: "valid_bet_USDT", Cny: "valid_bet_CNY"},
			{Base: "cashback", Usdt: "cashback_USDT", Cny: "cashback_CNY"},
			{Base: "profit_and_loss", Usdt: "profit_and_loss_USDT", Cny: "profit_and_loss_CNY"},
		},
	},
}

// ---------- helpers ----------

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func mapKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// ---------- 批次預撈主資料 ----------

func fetchRecordsBatch(ctx context.Context, db *gorm.DB, table string, ids []uint64, mapping FieldMapping, sets []AmountFieldSet) (map[uint64]recordRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	cols := []string{fmt.Sprintf("`%s` AS id", mapping.IDColumn)}
	includeCurDate := table != "acc_channel_info"
	if includeCurDate {
		cols = append(cols, "`currency`", "`entry_date`")
	}
	if mapping.SubCode != "" {
		cols = append(cols, fmt.Sprintf("`%s` AS sub_code", mapping.SubCode))
	}
	if mapping.SiteCode != "" {
		cols = append(cols, fmt.Sprintf("`%s` AS site_code", mapping.SiteCode))
	}

	// 若呼叫方傳入的 sets 為空，補上表的預設 AmountSets
	if len(sets) == 0 {
		sets = mapping.AmountSets
	}

	amountCols := map[string]struct{}{}
	for _, s := range sets {
		if s.Base != "" {
			amountCols[s.Base] = struct{}{}
		}
		if s.Usdt != "" {
			amountCols[s.Usdt] = struct{}{}
		}
		if s.Cny != "" {
			amountCols[s.Cny] = struct{}{}
		}
	}

	// 金額欄位為空，除 acc_channel_info 外都視為錯誤
	if table != "acc_channel_info" && len(amountCols) == 0 {
		log.Printf("[debug][%s] amountCols EMPTY | sets=%+v | mapping.AmountSets=%+v", table, sets, mapping.AmountSets)
		return map[uint64]recordRow{}, nil
	}

	amountColList := mapKeys(amountCols)
	sort.Strings(amountColList)
	log.Printf("[debug-cols][%s] amountColList=%v", table, amountColList)

	for _, c := range amountColList { // 這裡原本是 for c := range amountCols
		if c != "" {
			cols = append(cols, fmt.Sprintf("`%s`", c))
		}
	}

	sqlStr := fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` IN ? AND status = 2", strings.Join(cols, ","), table, mapping.IDColumn)
	rows, err := db.WithContext(ctx).Raw(sqlStr, ids).Rows()
	log.Printf("[debug-sql][%s] %s", table, sqlStr)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	recMap := make(map[uint64]recordRow, len(ids))
	for rows.Next() {
		var rr recordRow
		var sub, site sql.NullString
		scanTargets := []any{&rr.ID}
		if includeCurDate {
			scanTargets = append(scanTargets, &rr.Currency, &rr.EntryDate)
		}
		if mapping.SubCode != "" {
			scanTargets = append(scanTargets, &sub)
		}
		if mapping.SiteCode != "" {
			scanTargets = append(scanTargets, &site)
		}

		amountPtrs := make(map[string]*sql.NullFloat64, len(amountCols)) // 這行是關鍵，不能少
		for _, c := range amountColList {
			v := sql.NullFloat64{}
			amountPtrs[c] = &v
			scanTargets = append(scanTargets, amountPtrs[c])
		}

		if err := rows.Scan(scanTargets...); err != nil {
			return nil, err
		}
		if sub.Valid {
			rr.SubCode = sub.String
		}
		if site.Valid {
			rr.SiteCode = site.String
		}
		rr.Amounts = make(map[string]sql.NullFloat64, len(amountCols))
		for c, p := range amountPtrs {
			rr.Amounts[c] = *p
		}
		recMap[rr.ID] = rr
	}
	return recMap, nil
}

// ---------- 批次預撈辦公室 ----------

func prefetchOffices(ctx context.Context, db *gorm.DB, recMap map[uint64]recordRow) (map[string]officeInfo, map[string]officeInfo, error) {
	siteSet := map[string]struct{}{}
	subSet := map[string]struct{}{}
	for _, r := range recMap {
		if r.SiteCode != "" {
			siteSet[r.SiteCode] = struct{}{}
		}
		if r.SubCode != "" {
			subSet[r.SubCode] = struct{}{}
		}
	}
	siteMap := map[string]officeInfo{}
	subMap := map[string]officeInfo{}

	// site -> office
	if len(siteSet) > 0 {
		keys := mapKeys(siteSet)
		rows, err := db.WithContext(ctx).Raw(`
			SELECT t.site_code, m.main_code, m.name, s.sub_code, s.name, t.name
			FROM data_office_site t
			JOIN data_office_sub s ON s.id = t.office_sub_id
			JOIN data_office_main m ON m.id = s.office_main_id
			WHERE t.deleted_at IS NULL AND s.deleted_at IS NULL AND m.deleted_at IS NULL
			  AND t.site_code IN ?
			ORDER BY t.id DESC
		`, keys).Rows()
		if err != nil {
			return nil, nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var sc, mc, mn, sbc, sbn, stn string
			if err := rows.Scan(&sc, &mc, &mn, &sbc, &sbn, &stn); err != nil {
				return nil, nil, err
			}
			siteMap[sc] = officeInfo{
				MainCode: mc, MainOffice: mn,
				SubCode: sbc, SubOffice: sbn,
				SiteCode: sc, Site: sc,
			}
		}
	}

	// sub -> office
	if len(subSet) > 0 {
		keys := mapKeys(subSet)
		rows, err := db.WithContext(ctx).Raw(`
			SELECT s.sub_code, m.main_code, m.name, s.sub_code, s.name
			FROM data_office_sub s
			JOIN data_office_main m ON m.id = s.office_main_id
			WHERE s.deleted_at IS NULL AND m.deleted_at IS NULL
			  AND s.sub_code IN ?
			ORDER BY s.id DESC
		`, keys).Rows()
		if err != nil {
			return nil, nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var sc, mc, mn, sbc, sbn string
			if err := rows.Scan(&sc, &mc, &mn, &sbc, &sbn); err != nil {
				return nil, nil, err
			}
			subMap[sc] = officeInfo{
				MainCode: mc, MainOffice: mn,
				SubCode: sbc, SubOffice: sbn,
			}
		}
	}

	return siteMap, subMap, nil
}

// ---------- 批次預撈匯率（一次撈兩方向） ----------

type rateKey struct {
	Date string
	From string
	To   string
}

// 1) 統一 rate key：prefetchRates
func prefetchRates(ctx context.Context, db *gorm.DB, recMap map[uint64]recordRow) (map[rateKey]float64, error) {
	dateSet := map[string]struct{}{}
	curSet := map[string]struct{}{}
	for _, r := range recMap {
		if !r.EntryDate.Valid || !r.Currency.Valid {
			continue
		}
		dateSet[r.EntryDate.Time.Format("2006-01-02")] = struct{}{}
		cur := strings.ToUpper(strings.TrimSpace(r.Currency.String))
		curSet[cur] = struct{}{}
	}
	if len(dateSet) == 0 || len(curSet) == 0 {
		return map[rateKey]float64{}, nil
	}

	dates := mapKeys(dateSet)
	curs := mapKeys(curSet)

	rows, err := db.WithContext(ctx).Raw(`
        SELECT DATE(date_at) AS date_at, currency_from, currency_to, rate
        FROM sys_currency_rate_record
        WHERE deleted_at IS NULL
          AND currency_to IN ('CNY','USDT')
          AND DATE(date_at) IN ?
          AND currency_from IN ?
        ORDER BY id DESC
    `, dates, curs).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rateMap := map[rateKey]float64{}
	for rows.Next() {
		var d, f, t string
		var rate float64
		if err := rows.Scan(&d, &f, &t, &rate); err != nil {
			return nil, err
		}
		d = strings.TrimSpace(d)
		if len(d) >= 10 { // 確保只留日期
			d = d[:10]
		}
		k := rateKey{
			Date: d,
			From: strings.ToUpper(strings.TrimSpace(f)),
			To:   strings.ToUpper(strings.TrimSpace(t)),
		}
		if _, ok := rateMap[k]; !ok { // 依 id DESC，只收最新
			rateMap[k] = rate
		}
	}

	return rateMap, nil
}

// ---------- 辦公室/匯率查 cache ----------

func resolveOfficeCached(mapping FieldMapping, rec recordRow, siteMap, subMap map[string]officeInfo) (officeInfo, string) {
	if mapping.SiteCode != "" && rec.SiteCode != "" {
		if oi, ok := siteMap[rec.SiteCode]; ok {
			return oi, ""
		}
		return officeInfo{}, "office not found by site_code"
	}
	if mapping.SubCode != "" && rec.SubCode != "" {
		if oi, ok := subMap[rec.SubCode]; ok {
			return oi, ""
		}
		return officeInfo{}, "office not found by sub_code"
	}
	return officeInfo{}, ""
}

// 2) 自幣對自幣直接回 1，並標準化 from/to
func lookupRateCached(rateMap map[rateKey]float64, date time.Time, from, to string) (float64, error) {
	from = strings.ToUpper(strings.TrimSpace(from))
	to = strings.ToUpper(strings.TrimSpace(to))
	if from == to {
		return 1, nil
	}
	k := rateKey{Date: date.Format("2006-01-02"), From: from, To: to}
	if r, ok := rateMap[k]; ok {
		return r, nil
	}
	return 0, fmt.Errorf("lookupRate: no rate for %s %s->%s", k.Date, from, to)
}

// ---------- per-record 計算（用 cache，不打 DB） ----------
// 0206jamie: 調整 computeUpdateCached，
func computeUpdateCached(mapping FieldMapping, sets []AmountFieldSet, rec recordRow,
	siteMap, subMap map[string]officeInfo, rateMap map[rateKey]float64,
	table string, logger *log.Logger) (map[string]any, string) {

	office, officeReason := resolveOfficeCached(mapping, rec, siteMap, subMap)
	allOK := (officeReason == "")
	rateReason := ""
	update := map[string]any{}
	convertedCount := 0 // 至少有一個金額成功換算才算成功

	cur := strings.ToUpper(strings.TrimSpace(rec.Currency.String))
	dt := rec.EntryDate.Time

	for _, set := range sets {
		baseCol, usdtCol, cnyCol := set.Base, set.Usdt, set.Cny
		// 0212 金額欄位值為空（並非零)不做換算
		baseVal, ok := rec.Amounts[baseCol]
		if !ok {
			continue
		}

		// acc_channel_info 不做金額換算
		if table == "acc_channel_info" {
			continue
		}
		// 金額欄位為空(非零)，不做換算
		if !rec.Currency.Valid || !rec.EntryDate.Valid {
			allOK = false
			if !rec.Currency.Valid {
				rateReason = appendReason(rateReason, "currency NULL")
			}
			if !rec.EntryDate.Valid {
				rateReason = appendReason(rateReason, "entry_date NULL")
			}
			continue
		}

		base := baseVal.Float64
		amountCny := base
		amountUsdt := base
		rateOK := true
		rReason := ""

		switch cur {
		case "CNY":
			r, err := lookupRateCached(rateMap, dt, "CNY", "USDT")
			if err != nil {
				rateOK = false
				rReason = err.Error()
			} else {
				amountUsdt = round2(base * r)
			}
		case "USDT":
			r, err := lookupRateCached(rateMap, dt, "USDT", "CNY")
			if err != nil {
				rateOK = false
				rReason = err.Error()
			} else {
				amountCny = round2(base * r)
			}
		default:
			rCNY, err1 := lookupRateCached(rateMap, dt, cur, "CNY")
			rUSDT, err2 := lookupRateCached(rateMap, dt, cur, "USDT")
			if err1 != nil {
				rateOK = false
				rReason = err1.Error()
			} else if err2 != nil {
				rateOK = false
				rReason = err2.Error()
			} else {
				amountCny = round2(base * rCNY)
				amountUsdt = round2(base * rUSDT)
			}
		}

		if rateOK {
			update[cnyCol] = amountCny
			update[usdtCol] = amountUsdt
			convertedCount++
		} else {
			allOK = false
			rateReason = appendReason(rateReason, rReason)
		}
	}

	// 辦公/站點補齊
	if office.MainCode != "" && mapping.MainCode != "" {
		update[mapping.MainCode] = office.MainCode
	}
	if office.SubCode != "" && mapping.SubCode != "" {
		update[mapping.SubCode] = office.SubCode
	}
	if office.SiteCode != "" && mapping.SiteCode != "" {
		update[mapping.SiteCode] = office.SiteCode
	}
	if office.MainOffice != "" {
		update["main_office"] = office.MainOffice
	}
	if office.SubOffice != "" {
		update["sub_office"] = office.SubOffice
	}
	if office.Site != "" {
		update["site"] = office.Site
	}

	reasonText := buildReason(officeReason, rateReason)
	if allOK {
		update["status"] = 1
		update["recompute_info"] = nil
	} else {
		update["status"] = 2
		update["recompute_info"] = reasonText
	}
	return update, reasonText
}

func appendReason(cur, add string) string {
	if cur == "" {
		return add
	}
	if add == "" {
		return cur
	}
	return cur + "; " + add
}

func buildReason(officeReason, rateReason string) string {
	parts := []string{}
	if officeReason != "" {
		parts = append(parts, "office_reason="+officeReason)
	}
	if rateReason != "" {
		parts = append(parts, "rate_reason="+rateReason)
	}
	return strings.Join(parts, "; ")
}

// 批次 UPDATE：用 CASE 把多筆合成一條 SQL（無插入路徑）
func batchUpdate(ctx context.Context, db *gorm.DB, table, idCol string, rows []map[string]any, batchSize int, debug bool, logger *log.Logger) error {
	if len(rows) == 0 {
		return nil
	}

	ids := make([]any, 0, len(rows))
	cases := map[string][]string{}
	colArgs := map[string][]any{}

	for _, r := range rows {
		id := r[idCol]
		ids = append(ids, id)
		for col, val := range r {
			if col == idCol {
				continue
			}
			cases[col] = append(cases[col], "WHEN ? THEN ?")
			colArgs[col] = append(colArgs[col], id, val) // 參數順序：id, val
		}
	}

	// 固定欄位順序避免 map 無序
	cols := make([]string, 0, len(cases))
	for col := range cases {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	setClauses := make([]string, 0, len(cols))
	args := []any{}
	for _, col := range cols {
		setClauses = append(setClauses,
			fmt.Sprintf("`%s` = CASE `%s` %s ELSE `%s` END", col, idCol, strings.Join(cases[col], " "), col))
		args = append(args, colArgs[col]...) // 同欄位的參數依序加入
	}

	inPlaceholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	sqlStr := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` IN (%s)",
		table, strings.Join(setClauses, ", "), idCol, inPlaceholders)

	// CASE 的參數在前，IN (...) 的 id 參數接在後
	args = append(args, ids...)

	// 檢查 ? 數與參數數一致
	if strings.Count(sqlStr, "?") != len(args) {
		return fmt.Errorf("batchUpdate: placeholder mismatch sql ?=%d args=%d", strings.Count(sqlStr, "?"), len(args))
	}

	// 檢查批次長度
	if len(ids) != batchSize {
		logger.Printf("[WARN][%s] ids count %d != batchSize %d, ids=%v", table, len(ids), batchSize, ids)
		return fmt.Errorf("batchUpdate: ids count %d != batchSize %d", len(ids), batchSize)
	}

	if debug {
		logger.Printf("[SQL][%s] %s | args=%v", table, sqlStr, args)
	}
	return db.WithContext(ctx).Exec(sqlStr, args...).Error
}

// ---------- fetch IDs by batch ----------
func fetchIDsAfterID(ctx context.Context, db *gorm.DB, tbl, idCol, whereSQL string, args []any, batchSize int, lastID uint64) ([]uint64, error) {
	q := fmt.Sprintf("SELECT `%s` FROM `%s` WHERE (%s) AND `%s` > ? ORDER BY `%s` ASC LIMIT ?", idCol, tbl, whereSQL, idCol, idCol)
	rows, err := db.WithContext(ctx).Raw(q, append(args, lastID, batchSize)...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]uint64, 0)
	for rows.Next() {
		var v sql.NullInt64
		if scanErr := rows.Scan(&v); scanErr != nil {
			return nil, scanErr
		}
		if v.Valid {
			ids = append(ids, uint64(v.Int64))
		}
	}
	return ids, nil
}

// ---------- per-table loop ----------
func handleTable(ctx context.Context, db *gorm.DB, table string, batchSize int, debug bool, logger *log.Logger) bool {
	mapping, ok := TableFieldMappings[table]
	if !ok {
		logger.Printf("[%s] mapping not found, skip", table)
		return false
	}
	if mapping.IDColumn == "" {
		mapping.IDColumn = "id"
	}
	sets := mapping.AmountSets
	if len(sets) == 0 {
		sets = []AmountFieldSet{{Base: mapping.BaseAmount, Usdt: mapping.UsdtAmount, Cny: mapping.CnyAmount}}
	}
	logger.Printf("[debug-1][%s] sets len=%d sample=%+v", table, len(sets), sets)

	lastID := uint64(0)
	whereSQL := "status = 2"
	if table != "acc_channel_info" {
		whereSQL += " AND entry_date IS NOT NULL AND currency IS NOT NULL AND currency <> ''"
	}
	anyProcessed := false

	for {
		ids, err := fetchIDsAfterID(ctx, db, table, mapping.IDColumn, whereSQL, nil, batchSize, lastID)
		if err != nil {
			logger.Printf("[%s] fetch ids error: %v", table, err)
			return true
		}
		if len(ids) == 0 {
			return anyProcessed
		}
		anyProcessed = true
		lastID = ids[len(ids)-1]
		logger.Printf("[%s] batch size=%d range=%d-%d", table, len(ids), ids[0], lastID)

		// 預撈
		recMap, err := fetchRecordsBatch(ctx, db, table, ids, mapping, sets)
		if err != nil {
			logger.Printf("[%s] fetch records batch error: %v", table, err)
			continue
		}
		siteMap, subMap, err := prefetchOffices(ctx, db, recMap)
		if err != nil {
			logger.Printf("[%s] prefetch offices error: %v", table, err)
			continue
		}
		rateMap, err := prefetchRates(ctx, db, recMap)
		if err != nil {
			logger.Printf("[%s] prefetch rates error: %v", table, err)
			continue
		}

		updatesBatch := make([]map[string]any, 0, len(ids))
		updateCols := map[string]struct{}{}

		for _, id := range ids {
			rec, ok := recMap[id]
			if !ok {
				continue
			}
			upd, reason := computeUpdateCached(mapping, sets, rec, siteMap, subMap, rateMap, table, logger)
			if len(upd) == 0 {
				logger.Printf("[recompute][%s][%d] skip: %s", table, id, reason)
				continue
			}
			upd[mapping.IDColumn] = id
			updatesBatch = append(updatesBatch, upd)
			for k := range upd {
				if k != mapping.IDColumn {
					updateCols[k] = struct{}{}
				}
			}
		}

		if len(updatesBatch) == 0 {
			continue
		}

		// 快車道：批次 UPDATE（無插入路徑）
		err = batchUpdate(ctx, db, table, mapping.IDColumn, updatesBatch, batchSize, debug, logger)

		if err != nil {
			logger.Printf("[recompute][%s] batch update failed: %v, fallback to per-row", table, err)
			for _, row := range updatesBatch { // 慢車道
				id := row[mapping.IDColumn]
				delete(row, mapping.IDColumn)
				res := db.Table(table).Where(fmt.Sprintf("%s = ? AND status = 2", mapping.IDColumn), id).Updates(row)
				if res.Error != nil {
					logger.Printf("[recompute][%s][%v] slow-path error: %v", table, id, res.Error)
				}
			}
		}

	}
}

// ---------- main ----------

func main() {
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		fmt.Println("load config error:", err)
		return
	}
	logger := newLogger(cfg.Dirs.Logs)
	log.SetOutput(logger.Writer()) // 讓 log.Printf 也寫進同一個檔案

	logger.Printf("start recompute batch_size=%d", cfg.RecomputeBatchSize)

	// 載入 config 後
	debug := cfg.IsDebug == 1

	db, err := gorm.Open(mysql.Open(cfg.Database.Development.DSN), &gorm.Config{
		Logger: glogger.Default.LogMode(
			func() glogger.LogLevel {
				if debug {
					return glogger.Info // 印 SQL
				}
				return glogger.Silent // 不印 SQL
			}()),
	})

	if err != nil {
		logger.Printf("db connect error: %v", err)
		return
	}

	ctx := context.Background()

	/* 2024-02-09 Fix connection leak: Start */
	sqlDB, err := db.DB()
	if err != nil {
		logger.Printf("get sql.DB error: %v", err)
		return
	}
	// 設定連線池限制，避免長期佔用造成 500 error
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetConnMaxLifetime(time.Hour)
	/* 2024-02-09 Fix connection leak: End */

	tables := []string{
		"acc_cashbook",
		"acc_expenses",
		"acc_borrow_lend",
		"acc_recharge_withdraw",
		"acc_channel_info",
		"acc_ad_performance_analysis",
		"acc_balance_sheet",
		"acc_revenue_expense_adjustments",
		"acc_operational_information",
	}

	for {
		anyPending := false
		for _, tbl := range tables {
			time.Sleep(time.Second)
			if handleTable(ctx, db, tbl, cfg.RecomputeBatchSize, debug, logger) {
				anyPending = true
			}

		}
		now := time.Now().UTC().Format(time.RFC3339)
		if !anyPending {
			logger.Printf("[HEARTBEAT] %s tables=all status=idle", now)
			time.Sleep(30 * time.Second)
		} else {
			logger.Printf("[HEARTBEAT] %s tables=all status=pending", now)
		}
	}
}
