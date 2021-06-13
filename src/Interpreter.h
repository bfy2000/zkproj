#pragma once
#ifndef _INTERPRETER_
#define _INTERPRETER_
#include"MiniSQL.h"
//处理谓词,输入头尾不含空格的整个条件语句，例："a=100 and b=200.25 and c="250""
vector<select_predicate> handle_predicate(string& str);
/*sql语句类，每一个类代表一条sql语句，execute方法由API负责*/
class sql_create_table {
private:
	string table_name;
	map<string, column> columns;//<列名,column类>
	string pk_name;
public:
	sql_create_table(string sqlstmt);
	int execute();//执行
};

class sql_quit {
public:
	int execute();//执行
};

class sql_execfile {
private:
	string filename;
public:
	sql_execfile(string sqlstmt);
	int execute();
};
class sql_drop_table {
private:
	string table_name;
public:
	sql_drop_table(string sqlstmt);
	int execute();
};
class sql_create_index {
private:
	string table_name;
	string index_name;
	string column_name;
public:
	sql_create_index(string sqlstmt);
	int execute();
};
class sql_drop_index {
private:
	string index_name;
public:
	sql_drop_index(string sqlstmt);
	int execute();
};



class sql_select {
private:
	string table_name;
	vector<select_predicate> predicates;
public:
	sql_select(string sqlstmt);
	res_set execute();
};
class sqldata {//数据类型，包含数据种类和值（以string形式表示）
public:
	datatype type;
	string value;
public:
	sqldata(datatype type, string value) {
		this->type = type;
		this->value = value;
	}
};
class sql_insert {
private:
	string table_name;
	vector<sqldata> values;//存储类型和值，同样以字符串形式
public:
	sql_insert(string sqlstmt);
	int execute();
	//将数据连接成一个字符串，以做插入
	string connect();
	//获取列的数据
	string getcoldata(string columnname);
};
class sql_delete_from {
private:
	string table_name;
	vector<select_predicate> predicates;
public:
	sql_delete_from(string sqlstmt);
	int execute();
};

/*构造函数实现*/

sql_create_table::sql_create_table(string sqlstmt) {//传入参数为整条mysql语句，不含结尾分号，在一个string对象中，如"create table student (sno char(8),sname char(16) unique,sage int,sgender char(1),primary key(sno))"
	this->pk_name = "";
	string filter = "  ";
	str_filter(sqlstmt, filter);
	int c_start = sqlstmt.find_first_of('(');
	int c_end = sqlstmt.find_last_of(')');//提取列定义内容
	string t_name = sqlstmt.substr(13, sqlstmt.find_first_of('(') - 13);
	str_filter(t_name, ' ');
	this->table_name = t_name;

	

	
	string column_defines = sqlstmt.substr(c_start + 1, c_end - c_start - 1);
	vector<string> tokens = str_tokenizer(column_defines, ",");

	
	for (int i = 0; i < tokens.size();i++) {
		string temp = tokens[i];
		str_filter(temp, ' ');
		if (temp.find("primary key") != string::npos) {//该token含有主键定义
			if (this->pk_name != "") {//已经有主键
				string errmsg = "Multiple primary key definition is not allowed, current primary key name:" + this->pk_name + ".";
				throw errmsg;
			}
			string pkname = temp.substr(11);
			str_filter(pkname, ' ');
			int pk_start = pkname.find_first_of('(');
			int pk_end = pkname.find_last_of(')');
			if (pk_start == string::npos || pk_end == string::npos || pk_start > pk_end) {
				string errmsg = "You have an error in your sql syntax around" + pkname + ".";
				throw errmsg;
			}
			string pk = pkname.substr(pk_start + 1, pk_end - pk_start - 1);
			if (columns.find(pk) == columns.end()) {//找不到名称对应的列名
				string errmsg = "Can't find column name " + pk + " when trying to set primary key.";
				throw errmsg;
			}
			else {
				columns[pk].setpk();
				this->pk_name = pk;
			}
			
		}
		else {
			char *col_def = (char*)temp.c_str();
			column col(col_def);
			if (columns.find(col.name) != columns.end()) {//已有该名称的列
				string errmsg = "Column name \"" + col.name + "\" duplicated.";
				throw errmsg;
			}
			else {
				this->columns[col.name] = col;
			}
		}
	}

}
sql_create_index::sql_create_index(string sqlstmt) {
	str_filter(sqlstmt, ' ');
	string filter = "  ";
	str_filter(sqlstmt, filter);
	char delim = ' ';
	char* tokens = strtok((char*)(sqlstmt.c_str()), &delim);//create
	tokens = strtok(NULL, &delim);//index
	tokens = strtok(NULL, &delim);//index名称
	if (!tokens) {
		string errmsg = "Index name is needed.";
		throw errmsg;
	}
	this->index_name = (string)tokens;
	tokens = strtok(NULL, &delim);//on
	if (!tokens || strcmp(tokens, "on") != 0) {
		string errmsg = "You have an error in your sql syntax in \"" + sqlstmt + "\".";
		throw errmsg;
	}
	tokens = strtok(NULL, &delim);
	if (!tokens) {
		string errmsg = "Table name is needed.";
		throw errmsg;
	}
	this->table_name = (string)tokens;
	string c_name = str_extract(sqlstmt, '(', ')');
	str_filter(c_name, ' ');
	if (c_name.empty()) {
		string errmsg = "Column name is required.";
		throw errmsg;
	}
	else {
		this->column_name = c_name;
	}

}

sql_drop_table::sql_drop_table(string sqlstmt) {
	string filter = "  ";
	str_filter(sqlstmt, ' ');
	str_filter(sqlstmt, filter);
	string t_name = sqlstmt.substr(sqlstmt.find_last_of(' ') + 1);
	if (t_name.empty()) {
		string errmsg = "Table name is needed!";
		throw errmsg;
	}
	else
		this->table_name = t_name;


}
sql_drop_index::sql_drop_index(string sqlstmt) {
	str_filter(sqlstmt, ' ');
	string filter = "  ";
	str_filter(sqlstmt, filter);
	char delim = ' ';
	char* tokens = strtok((char*)(sqlstmt.c_str()), &delim);
	tokens = strtok(NULL, &delim);
	tokens = strtok(NULL, &delim);
	string idx_name = (string)tokens;
	if (!tokens) {
		string errmsg = "Index name is required.";
		throw errmsg;
	}
	else {
		tokens = strtok(NULL, &delim);
		if (tokens) {
			string errmsg = "You have an error in your sql syntax in \"" + sqlstmt + "\". Unexpected key detected.";
			throw errmsg;
		}
		else {
			this->index_name = idx_name;
		}
	}
}
sql_select::sql_select(string sqlstmt) {
	string token = str_sub(sqlstmt, " ");
	vector<string> keys;
	while (!sqlstmt.empty()) {
		if (!token.empty()) {//token有值
			keys.push_back(token);
			if (token == "where")
				break;
		}

		token = str_sub(sqlstmt, " ");
	}
	if (keys.back() != "where") {//没有where子句,则最后一个token是表名
		this->table_name = token;
	}
	else {//有where子句
		str_filter(sqlstmt, ' ');
		this->table_name = keys[3];
		this->predicates = handle_predicate(sqlstmt);
	}
}
sql_insert::sql_insert(string sqlstmt) {
	string values = str_extract(sqlstmt, '(', ')');//列值
	str_filter(values, ' ');
	string token = str_sub(sqlstmt, " ");

	
	vector<string> keys;
	while (!sqlstmt.empty()) {
		int finish = 0;
		if (!token.empty()) {
			keys.push_back(token);
			if (token == "into") {
				finish = 1;
			}

		}
		token = str_sub(sqlstmt, " ");
		if (finish == 1) {
			this->table_name = token;
			keys.push_back(token);
			break;
		}
	}
	if (dbinfo.tables.count(this->table_name) == 0) {
		string errmsg = "Table \"" + this->table_name + "\" does not exist.";
		throw errmsg;
	}
	table& thetable = dbinfo.tables[this->table_name];
	vector<string> vars = str_tokenizer_quot(values, ",");
	map<string, column>::iterator it = thetable.columns.begin();
	for (auto var : vars) {
		string str_type;
		datatype tb_data_t = it->second.type;
		if (tb_data_t == Int) {
			str_type = "int";
		}
		else if (tb_data_t == Float) {
			str_type = "float";
		}
		else {
			str_type = "char";
		}
		str_filter(var, ' ');
		if (var.empty()) {//没有值
			if (it->second.is_pk || it->second.is_unique) {
				string errmsg = "This column is needed:" + it->second.name + ":" + str_type + ".";
				throw errmsg;
			}
			sqldata tmp(tb_data_t, "");
			this->values.push_back(tmp);
		}
		else if (var[0] == '\'') {
			string str = str_extract(var, '\'', '\'');
			if (tb_data_t != Char) {
				string errmsg = "Wrong data type for cloumn "+ it->second.name+ " expecting "+str_type+".";
				throw errmsg;
			}
			sqldata tmp(Char, str);
			this->values.push_back(tmp);
		}
		else if (var[0] == '"') {
			string str = str_extract(var, '"', '"');
			if (tb_data_t != Char) {
				string errmsg = "Wrong data type for cloumn " + it->second.name + " expecting " + str_type + ".";
				throw errmsg;
			}
			sqldata tmp(Char, str);
			this->values.push_back(tmp);
		}
		else if(is_int(var)){
			if (tb_data_t == Char) {
				string errmsg = "Wrong data type for cloumn " + it->second.name + " expecting " + str_type + ".";
				throw errmsg;
			}
			if (tb_data_t == Int) {
				sqldata tmp(Int, to_string(stoi_h(var)));
				this->values.push_back(tmp);
			}
			else {
				sqldata tmp(Float, to_string(stof_h(var)));
				this->values.push_back(tmp);
			}
			
		}
		else {
			if (tb_data_t != Float) {
				string errmsg = "Wrong data type for cloumn " + it->second.name + " expecting " + str_type + ".";
				throw errmsg;
			}
			if (!is_float(var)) {
				string errmsg = "Unknown data type for cloumn " + it->second.name + " expecting " + str_type + ".";
				throw errmsg;
			}
			sqldata tmp(Float, to_string(stof_h(var)));
			this->values.push_back(tmp);
		}
		it++;


			
	}


	

}

sql_delete_from::sql_delete_from(string sqlstmt) {
	string token = str_sub(sqlstmt, " ");
	vector<string> keys;
	while (!sqlstmt.empty()) {
		if (!token.empty()) {//token有值
			keys.push_back(token);
			if (token == "where")
				break;
		}

		token = str_sub(sqlstmt, " ");
	}
	if (keys.back() != "where") {//没有where子句,则最后一个token是表名
		this->table_name = token;
	}
	else {//有where子句
		str_filter(sqlstmt, ' ');
		this->table_name = keys[2];
		this->predicates = handle_predicate(sqlstmt);
	}
}
string sql_insert::connect() {
	string ret_str = "";
	table thetable = dbinfo.tables[this->table_name];
	map<string, column>::iterator it = thetable.columns.begin();
	for (auto data : this->values) {
		string tempdata = data.value;
		if (tempdata.size() > it->second.datasize) {//数据长度检验
			string errmsg = "Data length exceed limit, maximum:" + to_string(it->second.datasize) + ".";
			throw errmsg;
		}
		while (tempdata.size() < it->second.datasize) {
			tempdata.push_back(EMPTY);
		}
		ret_str += tempdata;
		it++;
	}
	return ret_str;
}
sql_execfile::sql_execfile(string sqlstmt) {
	str_sub(sqlstmt, " ");
	str_filter(sqlstmt, ' ');
	if (sqlstmt.find('"') != string::npos) {
		this->filename = str_extract(sqlstmt, '"', '"');
	}
	else {
		this->filename = sqlstmt;
	}
}




vector<select_predicate> handle_predicate(string& str) {
	string op[] = { "=","<","<=",">=",">","<>" };
	vector<select_predicate> predicates;
	str_filter(str, ' ');
	vector<string> vars = str_tokenizer_quot_strict(str, "and");//以and为截断，切割出各个token
	for (auto var : vars) {
		select_predicate tmp(var);
		predicates.push_back(tmp);
	}

	return predicates;
}


string sql_insert::getcoldata(string columnname) {
	int i = 0;
	if (dbinfo.tables.count(this->table_name) == 0) {
		string errmsg = "Table \"" + this->table_name + "\" does not exist.";
		throw errmsg;
	}
	table &thetable = dbinfo.tables[this->table_name];
	for (auto col : thetable.columns) {
		if (col.second.name == columnname) {
			return this->values[i].value;
		}
		i++;
	}
}
#endif // !_INTERPRETER_