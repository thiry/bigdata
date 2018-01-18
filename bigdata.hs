import Data.List.Split
import System.Environment
import Data.List
import Network
import System.IO (hGetLine,hPutStrLn,hClose,hFlush)
import Control.Monad (forever)
import Control.Concurrent
import Data.Char
import Data.List

type G  x = [(x,x)]
type G' x = [(x,[x])]

eta :: G' x -> G x
eta = concat.(map (\(x,ys)->map (\y->(x,y)) ys))

get :: Eq x => x -> G x -> [x]
get n = concat.(map (\(x,y)->
 if (x==n) then [y] else []))

eta':: Eq x => [x] -> G x -> G' x
eta' xs g = map (\x->(x,get x g)) xs

get':: Eq x => x -> G' x -> [x]
get' n [] = []
get' n ((x,ys):xs) = if (x==n) then ys else get' n xs

-- Usage: 
-- ./bigdata slave graph.txt 9000 &
-- ./bigdata query localhost 9000 Zulu
-- Or: 
-- ./bigdata slave graph-part1.txt 9000 &
-- ./bigdata slave graph-part2.txt 9001 &
-- time (.bigdata query localhost 9000 Zulu & .bigdata query localhost 9001 Zulu)
main = do
 a <- getArgs
 case a of
  ["slave", file, port]    -> slave file (read port :: PortNumber)
  ["query", host, port, q] -> query host (read port :: PortNumber) q

-- transform tsv to haskell db
main0 = do
 fl <- readFile "links.tsv"
 let ls = lines fl
 let ws = map words ls
 let rs = concat (map (\e->[(e!!0,e!!1)]) ws)
 writeFile "graph.txt" (show rs)

-- read
main1 = do -- 0m3.092s
 fl <- readFile "graph.txt"
 let g = read fl :: G String
 print (last g)

-- get
main2 = do -- 0m3.112s
 xs <- getArgs
 f  <- readFile "graph.txt"
 let g = read f :: G String
 let r = get (xs!!0) g
 print r

nodes g = nub (ns g)
 where ns [] = []
       ns ((x,y):xs) = x:y:(ns xs)

main3 = do -- 0m20.556s
 fl <- readFile "graph.txt"
 let g = read fl :: G String
 let r = nodes g
 print (last r)

main4 = do -- 0m21.500s
 fl <- readFile "graph.txt"
 let g = read fl :: G String
 let r = nodes g
 writeFile "nodes.txt" (show r)

main5 = do -- 0m3.072s
 fl  <- readFile "graph.txt"
 fl' <- readFile "nodes.txt"
 let g  = read fl :: G String
 let ns = read fl' :: [String]
 print (last ns)
 print (last g)

main6 = do -- 0m3.160s - O(main5) = 0,088
 fl  <- readFile "graph.txt"
 fl' <- readFile "nodes.txt"
 let g  = read fl :: G String
 let ns = read fl' :: [String]
 let r  = eta' ns g
 print (last r)

main7 = do -- 0m57.596s
 fl  <- readFile "graph.txt"
 fl' <- readFile "nodes.txt"
 let g  = read fl :: G String
 let ns = read fl' :: [String]
 let r  = eta' ns g
 writeFile "graph2.txt" (show r)

main8 = do -- 0m1.268s
 fl  <- readFile "graph2.txt"
 let g  = read fl :: G' String
 print (last g)

main9 = do -- 0m1.160s - O(main8) = 0
 xs <- getArgs
 fl  <- readFile "graph2.txt"
 let g = read fl :: G' String
 let r = get' (xs!!0) g
 print r

main10 = do -- 
 xs <- getArgs
 fl  <- readFile "graph2.txt"
 let g = read fl :: G' String
 let x = take 1000000000 (repeat (xs!!0))
 let r = last $ map (\v-> get' v g) x
 print r

-- split db g
main11 = do -- 0m3.092s
 fl <- readFile "graph.txt"
 let g = read fl :: G String
 let n = div (length g) 2
 let part1 = take n g
 let part2 = drop n g
 writeFile "graph-part1.txt" (show part1)
 writeFile "graph-part2.txt" (show part2)

slave :: String -> PortNumber -> IO ()
slave file port = withSocketsDo $ do
 fl  <- readFile file
 let g = read fl :: G String
 sock <- listenOn $ PortNumber port
 slavebody sock g
 
slavebody sock g = 
 forever $ do
  (handle, host, port) <- accept sock
  query <- hGetLine handle
  let r = get query g
  hPutStrLn handle (show r)
  hFlush handle
  hClose handle 

query h p q = withSocketsDo $ do
 let n = connectTo h (PortNumber p)
 hd <- n
 hPutStrLn hd q
 hFlush hd
 rep <- hGetLine hd
 hClose hd
 putStrLn rep

demo q = do
 query "localhost" 9000 q
 query "localhost" 9001 q

-- fdp -Tpng -o graph.png graph.dot
dot = do
 f <- readFile "graph-part2.txt"
 let db = read f :: G String
 let r = convert (drop 59600 db)
 writeFile "graph.dot" r

convert db = concat ["digraph G {\n",edges,"}\n"]
 where es = filter (\(x,y)->((filter (`elem` d) x)==[])&&((filter (`elem` d) y)==[])) db
       edges = concat (map (\(x,y)->concat ["\t",x,"->",y,";\n"]) es)
       d = "01234567890.-"

dot2 = do
 f <- readFile "graph.txt"
 let db' = read f :: G String
 cs <- path
 let db = filter (\(a,b)->(a `elem` cs)||(b `elem` cs)) db'
 let r = convert db
 writeFile "graph.dot" r

path = do
 f <- readFile "graph.txt"
 let db = read f :: G String
 let r = connected "Zulu" db
 let r2= concat (map (\x->connected x db) r)
 return (nub r2)

connected x [] = []
connected x ((a,b):xs) = if (a==x) then (b:xs') else
  if (b==x) then (a:xs') else xs'
 where xs' = connected x xs

-- ./bigdata demo Zulu
-- ./bigdata demo IPod
main14 = do
 a <- getArgs
 case a of
  ["slave", file, port]    -> slave file (read port :: PortNumber)
  ["query", host, port, q] -> query host (read port :: PortNumber) q
  ["demo", q] -> do
    forkIO $ query "localhost" 9000 q
    query "localhost" 9001 q

