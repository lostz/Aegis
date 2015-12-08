build:
	goyacc -o sql/sql_yacc.go -p MySQL sql/sql_yacc.yy
	rm -rf y.output
	sed -i "" 's/MySQLlex/yylex/' sql/sql_yacc.go;
	commithash=`git log --max-count=1 --format=%h`
	version=`echo $commitdesc | awk -F'-' '{print $1}'`
	rls=`echo $commitdesc | awk -F'-' '{print $2}'`
	go build -ldflags "-X main.BuildStamp=`date '+%Y-%m-%d-%H:%M:%S'` -X main.Version=$version-$rls -X main.CommitId=$commithash " -o aegis

