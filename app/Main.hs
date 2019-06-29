module Main where

import System.Environment
import Lib

main :: IO ()
main = do
  args <- getArgs
  readAllWpm (args !! 0) (args !! 1)
