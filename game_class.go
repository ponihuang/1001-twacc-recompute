package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"gorm.io/gorm"
)

const gameClassLookupBatch = 100
const gameClassNameCollation = "utf8mb4_unicode_ci"

type gameClassRow struct {
	Idx  int
	Name string
}

// prefetchGameClasses 沿用API的有效對照及資料庫定序。
// 回傳鍵保留原始類型；不以Go小寫／去重音等規則代替資料庫比對。
func prefetchGameClasses(ctx context.Context, db *gorm.DB, records map[uint64]recordRow) (map[string]string, error) {
	seen := make(map[string]struct{})
	for _, rec := range records {
		if rec.GameGenres.Valid && strings.TrimSpace(rec.GameGenres.String) != "" {
			seen[rec.GameGenres.String] = struct{}{}
		}
	}
	genres := make([]string, 0, len(seen))
	for genre := range seen {
		genres = append(genres, genre)
	}
	sort.Strings(genres)
	classes := make(map[string]string, len(genres))
	for from := 0; from < len(genres); from += gameClassLookupBatch {
		to := min(from+gameClassLookupBatch, len(genres))
		selects := make([]string, 0, to-from)
		args := make([]any, 0, to-from)
		for i := from; i < to; i++ {
			selects = append(selects, fmt.Sprintf(
				"(SELECT %d AS idx, c.name FROM data_game_class_refer m JOIN data_game_class c ON c.id = m.game_class_id WHERE m.deleted_at IS NULL AND c.deleted_at IS NULL AND m.game_name = CONVERT(? USING utf8mb4) COLLATE %s LIMIT 1)",
				i, gameClassNameCollation))
			args = append(args, genres[i])
		}
		var rows []gameClassRow
		if err := db.WithContext(ctx).Raw(strings.Join(selects, " UNION ALL "), args...).Scan(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.Idx >= from && row.Idx < to && strings.TrimSpace(row.Name) != "" {
				classes[genres[row.Idx]] = row.Name
			}
		}
	}
	return classes, nil
}

func resolveGameClassReason(genre sql.NullString, classes map[string]string) string {
	if !genre.Valid || strings.TrimSpace(genre.String) == "" {
		return "原始帳務未填寫【遊戲類型】"
	}
	if strings.TrimSpace(classes[genre.String]) != "" {
		return ""
	}
	return fmt.Sprintf("找不到【遊戲類型】%s對應的遊戲主類名稱，請至「遊戲分類對照」新增或修正名稱對照。", genre.String)
}
