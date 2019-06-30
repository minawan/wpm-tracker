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
  show (StatEntry rowId date wpm high) =
    intercalate "," $ [show rowId, show date, show wpm, show high]

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

checkPred1 :: (StatEntry -> Bool)
           -> (StatEntry -> String)
           -> StatEntry -> [String]
checkPred1 p msgGen entry
  | p entry = []
  | otherwise = [msgGen entry]

checkPred2 :: (StatEntry -> StatEntry -> Bool)
           -> (StatEntry -> StatEntry -> String)
           -> StatEntry -> StatEntry -> [String]
checkPred2 p msgGen entry1 entry2
  | p entry1 entry2 = []
  | otherwise = [msgGen entry1 entry2]

checkHighGeWpm :: StatEntry -> [String]
checkHighGeWpm = checkPred1 p msgGen
  where p (StatEntry _ _ wpm high) = wpm <= high
        msgGen (StatEntry rowId _ wpm high) =
          printf "Row %d: high=%d < wpm=%d" rowId high wpm

checkSequentialRowId :: StatEntry -> StatEntry -> [String]
checkSequentialRowId = checkPred2 p msgGen
  where p (StatEntry rowId1 _ _ _) (StatEntry rowId2 _ _ _) =
          rowId1 == rowId2 - 1 
        msgGen (StatEntry rowId1 _ _ _) (StatEntry rowId2 _ _ _) =
          printf "Non-sequential row IDs %d and %d" rowId1 rowId2

checkDatesInOrder :: StatEntry -> StatEntry -> [String]
checkDatesInOrder = checkPred2 p msgGen
  where p (StatEntry _ date1 _ _) (StatEntry _ date2 _ _) = date1 <= date2 
        msgGen (StatEntry rowId1 date1 _ _) (StatEntry rowId2 date2 _ _) =
          printf "Dates not in order: row%d.date=%s > row%d.date=%s"
                 rowId1 (show date1) rowId2 (show date2)

checkRunningMax :: StatEntry -> StatEntry -> [String]
checkRunningMax = checkPred2 p msgGen
  where p (StatEntry _ _ _ high1) (StatEntry _ _ wpm2 high2) =
          high2 == max high1 wpm2
        msgGen (StatEntry rowId1 _ _ high1) (StatEntry rowId2 _ wpm2 high2) =
          printf "Invalid running max: row%d.high=%d != " rowId2 high2
          ++ printf "max row%d.high=%d " rowId1 high1
          ++ printf "row%d.wpm=%d == %d" rowId2 wpm2 (max high1 wpm2)

checkContiguousEntries :: StatEntry -> StatEntry -> [String]
checkContiguousEntries entry1 entry2 = checkSequentialRowId entry1 entry2
                                    ++ checkDatesInOrder entry1 entry2
                                    ++ checkRunningMax entry1 entry2

validateStatEntries :: [StatEntry] -> [String]
validateStatEntries [] = []
validateStatEntries [entry] = checkHighGeWpm entry
validateStatEntries (entry1:entry2:tl) = checkHighGeWpm entry1
                                      ++ checkContiguousEntries entry1 entry2
                                      ++ validateStatEntries (entry2:tl)
