module Lib
    ( StatEntry
    , readAllRows
    , validateStatEntries 
    ) where

import Data.List (intercalate)
import Data.Time.Calendar (Day)
import Data.Time.Format (defaultTimeLocale, parseTimeM)
import Database.HDBC (SqlValue, fromSql)
import Text.Printf (printf)

type RowId = Integer
type Date = Day
type Wpm = Integer
type High = Integer

data StatEntry = StatEntry RowId Date Wpm High

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

checkHighGeWpm :: StatEntry -> [String]
checkHighGeWpm (StatEntry rowId _ wpm high)
  | wpm <= high = []
  | otherwise = [printf "Row %d: high=%d < wpm=%d" rowId high wpm]

checkSequentialRowId :: StatEntry -> StatEntry -> [String]
checkSequentialRowId (StatEntry rowId1 _ _ _) (StatEntry rowId2 _ _ _)
  | rowId1 == rowId2 - 1 = []
  | otherwise = [msg]
  where msg = printf "Non-sequential row IDs %d and %d" rowId1 rowId2

checkDatesInOrder :: StatEntry -> StatEntry -> [String]
checkDatesInOrder (StatEntry rowId1 date1 _ _) (StatEntry rowId2 date2 _ _)
  | date1 <= date2 = []
  | otherwise = [msg]
  where msg = printf "Dates not in order: row%d.date=%s > row%d.date=%s"
                     rowId1 (show date1) rowId2 (show date2)

checkRunningMax :: StatEntry -> StatEntry -> [String]
checkRunningMax (StatEntry rowId1 _ _ high1) (StatEntry rowId2 _ wpm2 high2)
  | high2 == runningMax = []
  | otherwise = [msg]
  where runningMax = max high1 wpm2
        msg = printf "Invalid running max: row%d.high=%d != " rowId2 high2
           ++ printf "max row%d.high=%d " rowId1 high1
           ++ printf "row%d.wpm=%d == %d" rowId2 wpm2 runningMax


checkContiguousEntries :: StatEntry -> StatEntry -> [String]
checkContiguousEntries entry1 entry2 = checkSequentialRowId entry1 entry2
                                    ++ checkDatesInOrder entry1 entry2
                                    ++ checkRunningMax entry1 entry2

validateStatEntries :: [StatEntry] -> [String]
validateStatEntries [] = []
validateStatEntries [entry] = checkHighGeWpm entry
validateStatEntries (entry1:entry2:tl) = checkHighGeWpm entry1 ++ checkContiguousEntries entry1 entry2 ++ validateStatEntries (entry2:tl)
