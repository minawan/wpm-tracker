module Lib
    ( StatEntry
    , readAllRows
    ) where

import Data.List (intercalate)
import Data.Time.Calendar (Day)
import Data.Time.Format (defaultTimeLocale, parseTimeM)
import Database.HDBC (SqlValue, fromSql)

type RowId = Integer
type Date = Day
type Wpm = Integer
type High = Integer

data StatEntry = StatEntry { rowIdField :: RowId
                           , dateField :: Date
                           , wpmField :: Wpm
                           , highField :: High
                           }

instance Show StatEntry where
  show (StatEntry rowId date wpm high) = intercalate "," $ [show rowId, show date, show wpm, show high]

convRow :: [SqlValue] -> Maybe StatEntry
convRow [sqlRowId, sqlDate, sqlWpm, sqlHigh] =
    case dateM of
      Just date -> Just $ StatEntry rowId date wpm high
      Nothing -> Nothing
  where rowId = fromSql sqlRowId
        dateM = parseTimeM True defaultTimeLocale "%Y-%m-%d" $ fromSql sqlDate
        wpm = fromSql sqlWpm
        high = fromSql sqlHigh
convRow _ = Nothing

readAllRows :: [[SqlValue]] -> [Maybe StatEntry]
readAllRows = map convRow
