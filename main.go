package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	// Mode               string `yaml:"mode"`
	RecomputeBatchSize int `yaml:"recompute_batch_size"`
	IsDebug            int `yaml:"isdebug"` // 新增
	Database           struct {
		Development struct {
			// Dialect string `yaml:"dialect"`
			DSN string `yaml:"dsn"`
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
		MaxSize:    100,   //單一 log 檔達 100 MB 後輪替。
		MaxAge:     3,     //備份 log 最多保留 3 天。
		MaxBackups: 7,     //最多保留 7 個舊 log 檔。
		Compress:   false, //舊檔不壓縮。
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
	MainCode   string
	SubCode    string
	SiteCode   string
	IDColumn   string
	AmountSets []AmountFieldSet
}

type recordRow struct {
	ID             uint64
	Currency       sql.NullString
	EntryDate      sql.NullTime
	SubCode        string
	SiteCode       string
	FromTable      string
	TargetSiteCode string
	Amounts        map[string]sql.NullFloat64
}

type officeInfo struct {
	MainCode   string
	SubCode    string
	SiteCode   string
	MainOffice string
	SubOffice  string
	Site       string
}
type recomputeRoundLog struct { // 0614_新增 recomputeRoundLog 表對應結構 start
	ID        uint64     `gorm:"column:id;primaryKey"`
	Name      string     `gorm:"column:name"`
	Status    string     `gorm:"column:status"`
	StartedAt time.Time  `gorm:"column:started_at"`
	EndedAt   *time.Time `gorm:"column:ended_at"`
}

func (recomputeRoundLog) TableName() string {
	return "log_recompute_round"
}

type recomputeRoundTableLog struct {
	ID            uint64     `gorm:"column:id;primaryKey"`
	RoundID       uint64     `gorm:"column:round_id"`
	SourceTable   string     `gorm:"column:table_name"`
	StartedAt     time.Time  `gorm:"column:started_at"`
	EndedAt       *time.Time `gorm:"column:ended_at"`
	UpdateIDCount uint64     `gorm:"column:update_id_count"`
}

func (recomputeRoundTableLog) TableName() string {
	return "log_recompute_round_table"
} // 0614_新增 recomputeRoundLog 表對應結構 end

// ---------- table mappings 全表定義---------
var TableFieldMappings = map[string]FieldMapping{
	"acc_cashbook": {
		MainCode: "main_office", SubCode: "sub_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			{Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"acc_expenses": {
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			// {Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"acc_borrow_lend": {
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			// {Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"acc_recharge_withdraw": {
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
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
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
	},
	"acc_ad_performance_analysis": {
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
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
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
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
			// {Base: "converted_amount", Usdt: "converted_amount_USDT", Cny: "converted_amount_CNY"},
			// {Base: "balance_verification", Usdt: "balance_verification_USDT", Cny: "balance_verification_CNY"},
			// {Base: "difference", Usdt: "difference_USDT", Cny: "difference_CNY"},
		},
	},
	"acc_revenue_expense_adjustments": {
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			{Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"acc_operational_information": {
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "valid_bet", Usdt: "valid_bet_USDT", Cny: "valid_bet_CNY"},
			{Base: "cashback", Usdt: "cashback_USDT", Cny: "cashback_CNY"},
			{Base: "profit_and_loss", Usdt: "profit_and_loss_USDT", Cny: "profit_and_loss_CNY"},
		},
	},
	"acc_operational_information_excel": {
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			{Base: "bet_amount", Usdt: "bet_amount_usdt", Cny: "bet_amount_cny"},
		},
	},
	"acc_recharge_withdraw_excel": {
		MainCode: "main_office", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
			// {Base: "converted_amount", Usdt: "converted_amount_usdt", Cny: "converted_amount_cny"},
		},
	},
	"account_summary": {
		MainCode: "main_office", SubCode: "sub_code", SiteCode: "site_code", IDColumn: "id",
		AmountSets: []AmountFieldSet{
			{Base: "amount", Usdt: "amount_usdt", Cny: "amount_cny"},
		},
	},
}

// ---------- helpers ----------

// 0706 需求為不算小數位不四捨五入。
func roundAmount(table, col string, v float64) float64 {
	return v
}

func normalizeCode(value string) string { // 統一代碼正規化。
	return strings.ToLower(strings.TrimSpace(value))
}

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
	includeFromTable := table == "account_summary"
	includeTargetSiteCode := table == "account_summary"

	if includeCurDate {
		cols = append(cols, "`currency`", "`entry_date`")
	}
	if includeFromTable { // 小計表用
		cols = append(cols, "`from_table`")
	}
	if includeTargetSiteCode { // 小計表用
		cols = append(cols, "`target_site_code`")
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

	// 金額欄位為空且不是 acc_channel_info 時，直接回空結果，避免撈整張表。 acc_channel_info要補齊辦公室資訊。
	if table != "acc_channel_info" && len(amountCols) == 0 {
		log.Printf("[debug][%s] amountCols EMPTY | sets=%+v | mapping.AmountSets=%+v", table, sets, mapping.AmountSets)
		return map[uint64]recordRow{}, nil
	}

	amountColList := mapKeys(amountCols)
	sort.Strings(amountColList)

	for _, c := range amountColList {
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
		var sub, site, fromTable, targetSiteCode sql.NullString
		scanTargets := []any{&rr.ID}

		if includeCurDate {
			scanTargets = append(scanTargets, &rr.Currency, &rr.EntryDate)
		}
		if includeFromTable {
			scanTargets = append(scanTargets, &fromTable)
		}
		if includeTargetSiteCode {
			scanTargets = append(scanTargets, &targetSiteCode)
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

		if fromTable.Valid {
			rr.FromTable = normalizeCode(fromTable.String)
		}

		if targetSiteCode.Valid {
			rr.TargetSiteCode = normalizeCode(targetSiteCode.String)
		}

		if sub.Valid {
			rr.SubCode = normalizeCode(sub.String)
		}

		if site.Valid {
			rr.SiteCode = normalizeCode(site.String)
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
		siteKey := normalizeCode(r.SiteCode)
		if siteKey != "" {
			siteSet[siteKey] = struct{}{}
		}

		subKey := normalizeCode(r.SubCode)
		if subKey != "" {
			subSet[subKey] = struct{}{}
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
			siteKey := normalizeCode(sc)

			// ORDER BY id DESC，保留第一筆最新資料。
			if _, exists := siteMap[siteKey]; exists {
				continue
			}

			siteMap[siteKey] = officeInfo{
				MainCode:   mc,
				MainOffice: mn,
				SubCode:    sbc,
				SubOffice:  sbn,
				SiteCode:   sc,
				Site:       stn,
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

			subKey := strings.ToLower(strings.TrimSpace(sc))
			subMap[subKey] = officeInfo{
				MainCode: mc, MainOffice: mn,
				SubCode: sbc, SubOffice: sbn,
			}
		}
	}

	return siteMap, subMap, nil
}

// ---------- 批次預撈站點 ----------

func prefetchTargetSites(ctx context.Context, db *gorm.DB, recMap map[uint64]recordRow) (map[string]string, error) {

	targetSiteSet := map[string]struct{}{}

	for _, rec := range recMap {
		if rec.FromTable != "acc_borrow_lend" {
			continue
		}

		targetSiteKey := normalizeCode(rec.TargetSiteCode)
		if targetSiteKey != "" {
			targetSiteSet[targetSiteKey] = struct{}{}
		}
	}

	targetSiteMap := map[string]string{}
	if len(targetSiteSet) == 0 {
		return targetSiteMap, nil
	}

	keys := mapKeys(targetSiteSet)

	rows, err := db.WithContext(ctx).Raw(`
		SELECT site_code, name
		FROM data_office_site
		WHERE deleted_at IS NULL
		  AND site_code IN ?
		ORDER BY id DESC
	`, keys).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var siteCode, siteName string

		if err := rows.Scan(&siteCode, &siteName); err != nil {
			return nil, err
		}

		targetSiteKey := normalizeCode(siteCode)

		// ORDER BY id DESC，保留第一筆最新資料。
		if _, exists := targetSiteMap[targetSiteKey]; exists {
			continue
		}

		targetSiteMap[targetSiteKey] = siteName
	}

	return targetSiteMap, nil
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

func resolveOfficeCached(table string, mapping FieldMapping, rec recordRow, siteMap map[string]officeInfo, subMap map[string]officeInfo) (officeInfo, string) {

	// account_summary 來源是 acc_cashbook 時，使用 sub_code 查大小辦。
	if table == "account_summary" {
		if rec.FromTable == "acc_cashbook" {
			subKey := normalizeCode(rec.SubCode)
			if subKey == "" {
				return officeInfo{}, "原始帳務未填寫【小辦公室編號】"
			}
			if oi, ok := subMap[subKey]; ok {
				return oi, ""
			}
			return officeInfo{}, fmt.Sprintf("找不到【小辦公室編號】%s 對應的大辦公室名稱", rec.SubCode)
		} else {
			siteKey := normalizeCode(rec.SiteCode)
			if siteKey == "" {
				return officeInfo{}, "原始帳務未填寫【站點編號】"
			}
			if oi, ok := siteMap[siteKey]; ok {
				return oi, ""
			}
			return officeInfo{}, fmt.Sprintf("找不到【站點編號】%s 對應的大、小辦公室名稱", rec.SiteCode)
		}
	}
	// if table == "account_summary" && rec.FromTable == "acc_cashbook" {
	// 	subKey := normalizeCode(rec.SubCode)
	// 	if subKey == "" {
	// 		return officeInfo{}, "找不到【小辦公室編號】對應的大辦公室名稱"
	// 	}

	// 	if oi, ok := subMap[subKey]; ok {
	// 		return oi, ""
	// 	}

	// 	return officeInfo{}, fmt.Sprintf("找不到【小辦公室編號】%s 對應的大辦公室名稱", rec.SubCode)
	// }

	//	要放在其後 if table == "account_summary" && rec.FromTable == "acc_cashbook" {
	if mapping.SiteCode != "" && rec.SiteCode == "" {
		return officeInfo{}, "原始帳務未填寫【站點編號】"
	}
	//	要放在其後 if table == "account_summary" && rec.FromTable == "acc_cashbook" {
	if mapping.SubCode != "" && rec.SubCode == "" {
		return officeInfo{}, "原始帳務未填寫【小辦公室編號】"
	}

	// 其他情況優先使用 site_code。
	if mapping.SiteCode != "" && rec.SiteCode != "" {
		siteKey := normalizeCode(rec.SiteCode)

		if oi, ok := siteMap[siteKey]; ok {
			return oi, ""
		}

		return officeInfo{}, fmt.Sprintf("找不到【站點編號】%s 對應的大、小辦公室名稱", rec.SiteCode)
	}

	// 沒有 site_code 時，再使用 sub_code。
	if mapping.SubCode != "" && rec.SubCode != "" {

		subKey := normalizeCode(rec.SubCode)

		if oi, ok := subMap[subKey]; ok {
			return oi, ""
		}

		return officeInfo{}, fmt.Sprintf("找不到【小辦公室編號】%s 對應的大辦公室名稱", rec.SubCode)
	}

	// return officeInfo{}, ""
	return officeInfo{}, "大小辦公室名稱解析不到"
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

	return 0, fmt.Errorf("查無日期【%s】的【%s】匯率資料", k.Date, from)
}

// ---------- per-record 計算（用 cache，不打 DB） ----------
func computeUpdateCached(mapping FieldMapping, sets []AmountFieldSet, rec recordRow,
	siteMap, subMap map[string]officeInfo, targetSiteMap map[string]string, rateMap map[rateKey]float64,
	table string, logger *log.Logger) (map[string]any, string) {

	office, officeReason := resolveOfficeCached(table, mapping, rec, siteMap, subMap)

	allOK := (officeReason == "")
	rateReason := ""
	targetSiteReason := ""
	rateReasonSeen := map[string]struct{}{} //0213，避免同一筆資料因多個金額欄位缺匯率而重複累加原因
	update := map[string]any{}

	if table == "account_summary" &&
		rec.FromTable == "acc_borrow_lend" {

		targetSiteKey := normalizeCode(rec.TargetSiteCode)

		// if targetSiteKey == "" {
		// 	// allOK = false
		// 	// targetSiteReason = "原始帳務未填寫【對象站點編號】"
		// } else if targetSite, ok := targetSiteMap[targetSiteKey]; ok {
		// 	update["target_site"] = targetSite
		// } else {
		// 	allOK = false
		// 	targetSiteReason = fmt.Sprintf("找不到【對象站點編號】%s 對應的對象站點名稱", rec.TargetSiteCode)
		// }

		//0612_如果不存在不需要列錯誤，就留空即可。因為只有某些會計科目才會出現對象站點編號 並非全部
		if targetSiteKey != "" {
			if targetSite, ok := targetSiteMap[targetSiteKey]; ok {
				update["target_site"] = targetSite
			} else {
				allOK = false
				targetSiteReason = fmt.Sprintf("找不到【對象站點編號】%s 對應的對象站點名稱", rec.TargetSiteCode)
			}
		}
	}

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
		if !baseVal.Valid {
			update[usdtCol] = nil // base 為 NULL 時，衍生金額也設為 NULL
			update[cnyCol] = nil
			continue
		}
		//只用 status = 2 查詢，缺日期或幣別的資料會被處理。但要加上空字串幣別判為「幣別不存在」。
		if !rec.Currency.Valid || strings.TrimSpace(rec.Currency.String) == "" || !rec.EntryDate.Valid {
			allOK = false

			if !rec.Currency.Valid || strings.TrimSpace(rec.Currency.String) == "" {
				rateReason = appendReason(rateReason, "原始帳務未填寫【幣別】")
			}
			if !rec.EntryDate.Valid {
				rateReason = appendReason(rateReason, "原始帳務未填寫【帳務日期】")
			}
			continue
		} //缺日期／幣別 end

		//修改 base 金額四捨五入邏輯，改為先換算再四捨五入，避免因為 base 金額的小數位數不同而導致換算後金額不一致的問題。
		base := baseVal.Float64
		amountCny := roundAmount(table, cnyCol, base)
		amountUsdt := roundAmount(table, usdtCol, base)

		rateOK := true
		rReason := ""

		switch cur {
		case "CNY":
			r, err := lookupRateCached(rateMap, dt, "CNY", "USDT")
			if err != nil {
				rateOK = false
				rReason = err.Error()
			} else {
				amountUsdt = roundAmount(table, usdtCol, base*r)
			}
		case "USDT":
			r, err := lookupRateCached(rateMap, dt, "USDT", "CNY")
			if err != nil {
				rateOK = false
				rReason = err.Error()
			} else {
				amountCny = roundAmount(table, cnyCol, base*r)
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
				amountCny = roundAmount(table, cnyCol, base*rCNY)
				amountUsdt = roundAmount(table, usdtCol, base*rUSDT)
			}
		}

		if rateOK {
			update[cnyCol] = amountCny
			update[usdtCol] = amountUsdt
		} else {
			allOK = false
			// rateReason = appendReason(rateReason, rReason)
			// 0213 已改為只累一次原因，避免同一筆資料因多個金額欄位缺匯率而重複累加原因
			if rReason != "" {
				if _, ok := rateReasonSeen[rReason]; !ok {
					rateReasonSeen[rReason] = struct{}{}
					rateReason = appendReason(rateReason, rReason)
				}
			}
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
	reasonText = appendReason(reasonText, targetSiteReason)
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
		// parts = append(parts, "辦公室原因="+officeReason)
		parts = append(parts, "- "+officeReason)
	}
	if rateReason != "" {
		// parts = append(parts, "匯率原因="+rateReason)
		parts = append(parts, "- "+rateReason)
	}
	return strings.Join(parts, "; ")
}

// 批次 UPDATE：用 CASE 把多筆合成一條 SQL（無插入路徑）
func batchUpdate(ctx context.Context, db *gorm.DB, table, idCol string, rows []map[string]any, debug bool, logger *log.Logger) error {
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
	sqlStr := fmt.Sprintf("UPDATE `%s` SET %s WHERE status = 2 AND `%s` IN (%s)",
		table, strings.Join(setClauses, ", "), idCol, inPlaceholders)

	// CASE 的參數在前，IN (...) 的 id 參數接在後
	args = append(args, ids...)

	// 檢查 ? 數與參數數一致
	if strings.Count(sqlStr, "?") != len(args) {
		return fmt.Errorf("batchUpdate: placeholder mismatch sql ?=%d args=%d", strings.Count(sqlStr, "?"), len(args))
	}

	if debug {
		debugSQL := BuildDebugSQL(sqlStr, args)
		logger.Printf(
			// "[SQL][%s]\nraw_sql=%s\nargs=%v\ndebug_sql=%s",
			"[SQL][%s]\ndebug_sql=%s",
			table,
			// sqlStr,
			// args,
			debugSQL,
		)
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

// 0614_dblog 增加 DB LOG 函式 start
func startRoundLog(ctx context.Context, db *gorm.DB) (*recomputeRoundLog, error) {
	round := &recomputeRoundLog{
		Name:      "重算補齊",
		Status:    "processing",
		StartedAt: time.Now(),
	}

	if err := db.WithContext(ctx).Create(round).Error; err != nil {
		return nil, err
	}

	return round, nil
}

func finishRoundLog(
	ctx context.Context,
	db *gorm.DB,
	roundID uint64,
) error {
	endedAt := time.Now()

	return db.WithContext(ctx).
		Model(&recomputeRoundLog{}).
		Where("id = ?", roundID).
		Updates(map[string]any{
			"status":   "completed",
			"ended_at": endedAt,
		}).Error
}

func startTableLog(
	ctx context.Context,
	db *gorm.DB,
	roundID uint64,
	table string,
) (*recomputeRoundTableLog, error) {
	item := &recomputeRoundTableLog{
		RoundID:       roundID,
		SourceTable:   table,
		StartedAt:     time.Now(),
		UpdateIDCount: 0,
	}

	if err := db.WithContext(ctx).Create(item).Error; err != nil {
		return nil, err
	}

	return item, nil
}

func finishTableLog(
	ctx context.Context,
	db *gorm.DB,
	tableLogID uint64,
	updateIDCount uint64,
) error {
	endedAt := time.Now()

	return db.WithContext(ctx).
		Model(&recomputeRoundTableLog{}).
		Where("id = ?", tableLogID).
		Updates(map[string]any{
			"ended_at":        endedAt,
			"update_id_count": updateIDCount,
		}).Error
}

func deleteExpiredRoundLogs(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 先刪除明細，避免留下沒有主表的資料。
		if err := tx.Exec(`
			DELETE FROM log_recompute_round_table
			WHERE round_id IN (
				SELECT id
				FROM log_recompute_round
				WHERE started_at < NOW() - INTERVAL 3 DAY
			)
		`).Error; err != nil {
			return err
		}

		return tx.Exec(`
			DELETE FROM log_recompute_round
			WHERE started_at < NOW() - INTERVAL 3 DAY
		`).Error
	})
} // 0614_dblog 增加 DB LOG 函式 end

// ---------- per-table loop ----------
// 0614_dblog 回傳是否有處理資料，以及更新的資料筆數 start
// func handleTable(ctx context.Context, db *gorm.DB, table string, batchSize int, debug bool, logger *log.Logger) bool {
func handleTable(ctx context.Context, db *gorm.DB, table string, batchSize int, debug bool, logger *log.Logger) (bool, uint64) { //0614_dblog 回傳是否有處理資料，以及更新的資料筆數 end

	updateIDCount := uint64(0) //0614_dblog 更新的資料筆數

	mapping, ok := TableFieldMappings[table]
	if !ok {
		logger.Printf("[%s] mapping not found, skip", table)
		return false, updateIDCount //0614_dblog updateIDCount沒有 mapping 代表沒有處理資料，回傳 false 和 0
	}

	if mapping.IDColumn == "" {
		mapping.IDColumn = "id"
	}
	sets := mapping.AmountSets

	lastID := uint64(0)
	whereSQL := "status = 2"
	//只用 status = 2 查詢，缺日期或幣別的資料會被處理。但要加上空字串幣別判為「幣別不存在」。

	anyProcessed := false

	for {
		ids, err := fetchIDsAfterID(ctx, db, table, mapping.IDColumn, whereSQL, nil, batchSize, lastID)
		if err != nil {
			logger.Printf("[%s] fetch ids error: %v", table, err)
			// return true
			return true, updateIDCount //0614_dblog updateIDCount發生錯誤也算有處理資料，但更新筆數為 0
		}
		if len(ids) == 0 {
			return anyProcessed, updateIDCount //0614_dblog updateIDCount沒有資料了才回傳 false，否則回傳 true 代表有處理過資料
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

		targetSiteMap := map[string]string{}

		if table == "account_summary" {
			targetSiteMap, err = prefetchTargetSites(ctx, db, recMap)
			if err != nil {
				logger.Printf("[%s] prefetch target sites error: %v", table, err)
				continue
			}
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
			upd, reason := computeUpdateCached(mapping, sets, rec, siteMap, subMap, targetSiteMap, rateMap, table, logger)
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

		// 0614 計算實際送進 UPDATE 的 ID 數量。
		updateIDCount += uint64(len(updatesBatch))

		// 快車道：批次 UPDATE
		err = batchUpdate(ctx, db, table, mapping.IDColumn, updatesBatch, debug, logger)

		if err != nil {
			for _, row := range updatesBatch { // 慢車道
				id := row[mapping.IDColumn]
				delete(row, mapping.IDColumn)
				res := db.Table(table).Where(fmt.Sprintf("%s = ? AND status = 2", mapping.IDColumn), id).Updates(row)
				//印出嘗試慢車道更新的紀錄，方便後續分析是哪些資料有問題需要人工干預
				logger.Printf("[recompute][%s][%v] slow-path update attempted for columns %v, error: %v", table, id, mapKeys(updateCols), res.Error)

				if res.Error != nil {
					logger.Printf("[recompute][%s][%v] slow-path error: %v", table, id, res.Error)
				}
			}
		}

	}
}

// ---------- debug 組update的sql----------
func BuildDebugSQL(sqlStr string, args []interface{}) string {
	var b strings.Builder
	argIndex := 0

	for i := 0; i < len(sqlStr); i++ {
		if sqlStr[i] == '?' && argIndex < len(args) {
			b.WriteString(formatSQLValue(args[argIndex]))
			argIndex++
		} else {
			b.WriteByte(sqlStr[i])
		}
	}

	return b.String()
}

func formatSQLValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		return "'" + escapeSQLString(val) + "'"
	case []byte:
		return "'" + escapeSQLString(string(val)) + "'"
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05") + "'"
	case bool:
		if val {
			return "1"
		}
		return "0"
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%v", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", val)
	case float32, float64:
		return fmt.Sprintf("%v", val)
	default:
		return "'" + escapeSQLString(fmt.Sprintf("%v", val)) + "'"
	}
}

func escapeSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	return s
}

// // ---------- 寫入本輪完成時間，並清除三天前的紀錄。 0614 ----------
// func saveRoundLog(
// 	ctx context.Context,
// 	db *gorm.DB,
// 	startedAt time.Time,
// 	endedAt time.Time,
// ) error {
// 	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
// 		if err := tx.Exec(`
//             INSERT INTO log_recompute_round (started_at, ended_at)
//             VALUES (?, ?)
//         `, startedAt, endedAt).Error; err != nil {
// 			return fmt.Errorf("insert recompute round log: %w", err)
// 		}

// 		if err := tx.Exec(`
//             DELETE FROM log_recompute_round
//             WHERE ended_at < NOW() - INTERVAL 3 DAY
//         `).Error; err != nil {
// 			return fmt.Errorf("delete expired recompute round logs: %w", err)
// 		}

// 		return nil
// 	})
// }

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
		"acc_operational_information_excel",
		"acc_recharge_withdraw_excel",
		"account_summary",
	}

	// for {
	// 	roundStartedAt := time.Now() //0614_紀錄本輪開始時間
	// 	anyPending := false

	// 	for _, tbl := range tables {
	// 		time.Sleep(time.Second) // 每處理一個表休息1秒，讓其他系統有機會搶到 DB 連線，減少長時間佔用造成的 500 error

	// 		if handleTable(ctx, db, tbl, cfg.RecomputeBatchSize, debug, logger) {
	// 			anyPending = true
	// 		}
	// 	}
	// 	roundEndedAt := time.Now() //0614_紀錄本輪結束時間
	// 	if err := saveRoundLog(    // 0614_寫入本輪完成時間，並清除三天前的紀錄start
	// 		ctx,
	// 		db,
	// 		roundStartedAt,
	// 		roundEndedAt,
	// 	); err != nil {
	// 		logger.Printf("[ROUND] save log error: %v", err)
	// 	} else {
	// 		logger.Printf(
	// 			"[ROUND] completed started_at=%s ended_at=%s",
	// 			roundStartedAt.Format("2006-01-02 15:04:05"),
	// 			roundEndedAt.Format("2006-01-02 15:04:05"),
	// 		)
	// 	} //0614_寫入本輪完成時間，並清除三天前的紀錄end

	// 	now := time.Now().UTC().Format(time.RFC3339)
	// 	if !anyPending {
	// 		logger.Printf("[HEARTBEAT] %s tables=all status=idle(本輪沒待處理資料)", now)
	// 		time.Sleep(30 * time.Second) // 沒有待處理資料，休息30秒再檢查，避免空轉浪費資源
	// 	} else {
	// 		logger.Printf("[HEARTBEAT] %s tables=all status=pending(本輪有待處理資料)", now)
	// 	}
	// }
	//0614_改為每輪有一筆 log 紀錄，並且在每輪開始時就寫入資料庫，結束時更新狀態和結束時間，並刪除三天前的紀錄。這樣可以更準確地反映每輪的執行狀態和時間，並且避免在處理過程中發生錯誤導致 log 無法寫入的問題。
	for {
		roundLog, err := startRoundLog(ctx, db)
		if err != nil {
			logger.Printf("[ROUND] start log error: %v", err)
			time.Sleep(30 * time.Second)
			continue
		}

		anyPending := false

		for _, tbl := range tables {
			time.Sleep(time.Second)

			tableLog, logErr := startTableLog(ctx, db, roundLog.ID, tbl)
			if logErr != nil {
				logger.Printf(
					"[ROUND][%d][%s] start table log error: %v",
					roundLog.ID,
					tbl,
					logErr,
				)
			}

			processed, updateIDCount := handleTable(
				ctx,
				db,
				tbl,
				cfg.RecomputeBatchSize,
				debug,
				logger,
			)

			if processed {
				anyPending = true
			}

			if tableLog != nil {
				if logErr := finishTableLog(
					ctx,
					db,
					tableLog.ID,
					updateIDCount,
				); logErr != nil {
					logger.Printf(
						"[ROUND][%d][%s] finish table log error: %v",
						roundLog.ID,
						tbl,
						logErr,
					)
				}
			}
		}

		if err := finishRoundLog(ctx, db, roundLog.ID); err != nil {
			logger.Printf(
				"[ROUND][%d] finish log error: %v",
				roundLog.ID,
				err,
			)
		} else {
			logger.Printf("[ROUND][%d] completed", roundLog.ID)

			if err := deleteExpiredRoundLogs(ctx, db); err != nil {
				logger.Printf(
					"[ROUND][%d] delete expired logs error: %v",
					roundLog.ID,
					err,
				)
			}
		}

		now := time.Now().UTC().Format(time.RFC3339)

		if !anyPending {
			logger.Printf(
				"[HEARTBEAT] %s tables=all status=idle(本輪沒待處理資料)",
				now,
			)
			time.Sleep(30 * time.Second)
		} else {
			logger.Printf(
				"[HEARTBEAT] %s tables=all status=pending(本輪有待處理資料)",
				now,
			)
		}
	} //0614_改為每輪有一筆 log 紀錄，並且在每輪開始時就寫入資料庫，結束時更新狀態和結束時間，並刪除三天前的紀錄。這樣可以更準確地反映每輪的執行狀態和時間，並且避免在處理過程中發生錯誤導致 log 無法寫入的問題。

}
