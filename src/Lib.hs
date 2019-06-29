module Lib
    ( readAllWpm
    ) where

import Data.List (intercalate)
import Data.Time.Calendar (Day)
import Data.Time.Format (defaultTimeLocale, parseTimeM)
import Database.HDBC.Sqlite3 (connectSqlite3)
import Database.HDBC

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

readAllWpm :: String -> String -> IO ()
readAllWpm _ dbFilename = do
  conn <- connectSqlite3 dbFilename
  r <- quickQuery' conn
       "SELECT oid, * from stat ORDER BY oid" []
  let rows = case sequence $ map convRow r of
               Just rows -> rows
               Nothing -> []
  mapM_ (putStrLn . show) rows
  disconnect conn
