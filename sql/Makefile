build:
	goyacc -o sql_yacc.go -p MySQL sql_yacc.yy
	sed -i "" 's/MySQLlex/yylex/' sql_yacc.go;

