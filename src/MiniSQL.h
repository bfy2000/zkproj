#pragma once
#ifndef _MINISQL_H_
#define _MINISQL_H_



#include <stdio.h>
#include<iostream>
#include<string>
#include<vector>
#include<fstream>
#include<iomanip>
#include<map>
#include<cstdio>
#include"util.h"
#include<ctime>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include"MacroDefines.h"


using namespace std;
enum sqltype{create_table,drop_table,create_index,drop_index,select,insert,delete_from,quit,execfile};
enum datatype{Int, Char, Float};
enum operand{eq,lt,le,ge,gt,ne};//运算符,分别表示等于，小于，小于等于，大于等于，大于，不等于
enum sys_status{_Start,_SQL,_Command};//系统状态，开始界面，sql输入，系统命令输入
class select_predicate;
class column;
class sql_create_table;


//单个谓词比较器
bool comparator(select_predicate pd, string value);
//根据int返回对应数据类型
datatype reverse_type(int type);




/*
sql语句处理：读入用户输入语句后将每条语句（以";"分隔）转为1行，便于后续处理
传入类型判断函数，之后调用对应构造函数，生成语句对象，再调用execute()执行
*/
class OffsetType {
public:
	int blockOffset;
	int recordNum;
public:
	OffsetType() {
		blockOffset = 0;
		recordNum = 0;
	}
	OffsetType(int bof, int recnum) {
		blockOffset = bof;
		recordNum = recnum;
	}
};//type of offset value, int temporarily
typedef struct BPlusNode* BPTree, *Position;
typedef char* KeyType;
struct BPlusNode {
	int KeyNum;
	char* Keys[MaxKey + 1];
	OffsetType Offsets[MaxData + 1];		/* Pointers to key's offset in storage. This field is only used by leaf nodes. */
	BPlusNode* parent;
	BPTree Children[MaxKey + 1];
	BPTree Next;
};


class insertPos {//指明插入位置
public:
	int bufferNUM;
	int position;
};


class block {
public:
	char values[BLOCKSIZE+1];
	bool isWritten;
	bool isValid;
	string filename;
	int blockOffset;
	int LRUvalue;		//LRU
public:
	//成员函数...
	//读取指定范围数据
	block() {
		initialize();
	}
	string getvalues(int startpos, int endpos);
	char getvalues(int pos);
	void initialize();
};



class table {//储存表的定义信息,catalog manager负责
public:
	string tablename;
	map<string, column> columns;
	int block_num;
	vector<int> pk_unique;
	map<string,BPTree> hidden_BPT;//存放unique列b+树，用于插入时快速判断
	//map<string, string> indices;//<列名，索引名>
public:
	//从catalog文件读取到的信息中创建
	table(vector<string> tbl_def);
	table() {};
	//table(sql_create_table stmt);//从语句创建，创建后记住写入到catalog文件中，避免数据丢失
	table(string table_name, map<string, column> columns) {
		this->tablename = table_name;
		this->columns = columns;
		this->block_num = 0;
	}
	//~table();
	//获取一条记录总长度（byte）
	int getRecordLen();
	//将表信息化为标准catalog文件格式
	vector<string> Cataloginfo();
	//显示表的定义信息
	void show_tbl_info();
	//取得主键和独一列位置
	vector<int> get_PkUnique();
	//取得主键名称,没有返回空
	string get_pk();
	//获得pk位置，没有返回空
	int get_pkpos();
	//获得列位置，没有返回-1
	int get_col_pos(string col_name);
};





class index {//索引类，包含索引信息以及B+树根节点
public:
	string index_name;
	string table_name;
	string column_name;
	int block_num;
	BPTree BPT;
public:
	
	//~index();
	index() {};
	//从catalog文件读取信息
	index(string catinfo);
	OffsetType findkey(string key);
	int insertkey(table& thetable,string columnname, string key);
	int deletekey(string key);
	//生成标准输出到catalog文件内容
	vector<string> Cataloginfo();
	//打印索引信息
	void print_idxinfo();
	//保存到文件
	void savetofile();
	
};
class select_predicate {//谓词，查询条件
public:
	string column_name;
	operand op;
	datatype type;
	string value;//以字符串形式存储值，使用时需要进行转换
public:
	select_predicate(string predicate);//读入条件语句，例:"a=b"
	select_predicate() {

	}
};

class database {//数据库中的模式信息，由catalog manager负责，从catalog文件中读取
public:
	map<string, table> tables;
	map<string, index> indices;
	sys_status status;
public:
	//database(string filepath);
	//int write_to_file();//将数据库信息写到文件，应在每次有更新之后执行
	database() {
		status = _Start;
	}
	//显示所有表名
	void show_tables();
	//显示所有索引
	void show_indices();
	//保存所有索引到文件
	void save_all_indices() {
		for (auto idx : this->indices) {
			idx.second.savetofile();
		}
	}
};
database dbinfo;

class column {//列定义
public:
	string name;
	datatype type;
	int datasize;
	bool is_unique;
	bool is_pk;
public:
	//从sql语句中获得
	column(char* column_define);
	//从catalog文件中获得
	column(string column_define);
	column() {

	}
	int setpk();
	//转化为标准catalog文件格式
	string Cataloginfo();
	void operator=(const column& col) {
		this->datasize = col.datasize;
		name = col.name;
		type = col.type;
		is_unique = col.is_unique;
		is_pk = col.is_pk;
	}
};
class sql_tuple {
public:
	vector<string> row;
public:
	friend ostream& operator<<(ostream& os, const sql_tuple&);
	sql_tuple() {

	}
	sql_tuple(vector<string> vec) {
		this->row = vec;
	}
	void operator=(const sql_tuple& tup) {
		this->row = tup.row;
	}
	sql_tuple(string table_name, string raw_data);
	bool compare(vector<select_predicate> pred,table& thetable);
};

class res_set {//查询结果集
public:
	map<string,column> header;
	string table_name;
	vector<sql_tuple> tuples;
public:
	//输出结果集，直接使用cout
	friend ostream& operator<<(ostream& os,const res_set&);
	res_set(table& thetable) {
		this->table_name = thetable.tablename;
		this->header = thetable.columns;
	}
	res_set() {}
};

/*
//c_int, c_char, c_float 为具体数据对象，即代表着数据库中的某一个数据，record manager负责实现
class c_int :public column {
private:
	int value;
public:
	c_int(int v);
	int getvalue();
	void setvalue(int v);

};
class c_char :public column {
private:
	string value;
public:
	c_char(string str);
	c_char(char* str);
	string getvalue();
	void setvalue(string str);
	void setvalue(char* value);
};
class c_float :public column {
private:
	float value;
public:
	c_float(float v);
	float getvalue();
	void setvalue(float value);
};*/



void block::initialize() {
	isWritten = 0;
	isValid = 0;
	filename = "NULL";
	blockOffset = 0;
	LRUvalue = 0;
	for (int i = 0; i < BLOCKSIZE; i++) values[i] = EMPTY;
	values[BLOCKSIZE] = '\0';
}

string block::getvalues(int startpos, int endpos) {
	string tmpt = "";
	if (startpos >= 0 && startpos <= endpos && endpos <= BLOCKSIZE)
		for (int i = startpos; i < endpos; i++)
			tmpt += values[i];
	return tmpt;
}

char block::getvalues(int pos) {
	if (pos >= 0 && pos <= BLOCKSIZE)
		return values[pos];
	return '\0';
}



column::column(char* column_define) {//传入参数为干净的列定义，如 "sname char(16) unique"不含逗号等多余标点
	this->is_pk = false;
	this->is_unique = false;
	char delim = ' ';
	string str=column_define;
	string tokens = str_sub(str, " ");
		
		this->name = tokens;
		tokens = str_sub(str," ");
		string datatype = tokens;
		if (datatype == "int") {
			this->type = Int;
			this->datasize = INTLEN;
		}
		else if (datatype == "float") {
			this->type = Float;
			this->datasize = FLOATLEN;
		}else if (datatype.find("char")!=string::npos|| datatype == "char"){//含有char子串,如char(9)
			if (datatype == "char") {
				tokens = str_sub(str," ");
				datatype = tokens;
			}
			int start = datatype.find_first_of('(');
			int end = datatype.find_first_of(')');
			if (start == string::npos||end == string::npos||start>end) {
				string errmsg = "You have an error in your sql syntax at around" + tokens+".";
				throw errmsg;
			}
			string size = datatype.substr(start+1, end-start-1);
			if (stoi(size) <= 0) {
				string errormsg= "Char size can't be smaller than 0! Value:"+ stoi(size);
				throw errormsg;
			}
			this->datasize = stoi(size);
			this->type = Char;
		}
		else {
			string errormsg= "Invalid data type \""+tokens+"\" !";
			throw errormsg;
		}
		tokens = str_sub(str," ");
		if (!tokens.empty()) {
			string other=tokens;
			if (other == "unique") {
				this->is_unique = true;
			}
			else {
				string errmsg = "Unknown attribute " + other+".";
				throw errmsg;
			}
		}
	

}

int column::setpk() {
	if (this->is_pk) {//already set as pk
		string errmsg = this->name + " has already been set as primary key.";
		throw errmsg;
	}
	else {
		is_pk = true;
		return 1;
	}
}


sql_tuple::sql_tuple(string table_name, string raw_data) {
	
	table thetable= dbinfo.tables[table_name];
	int offset = 0;
	for (map<string, column>::iterator it = thetable.columns.begin(); it != thetable.columns.end(); it++) {
		int datasize = it->second.datasize;
		if (offset + datasize > raw_data.size()) {
			string errmsg = "Exception when creating tuple in result set:format data size exceed raw data size.";
			throw errmsg;
		}
		string tmp = raw_data.substr(offset, datasize);
		size_t last_emp = tmp.find_last_not_of(EMPTY);
		if (last_emp != string::npos) {
			tmp.erase( last_emp +1);
		}
		this->row.push_back(tmp);
		offset += datasize;
	}
	if (offset != raw_data.size())
		cout << "Warning: unsafe data transform, some raw data have not been used."<<endl;
}

int table::getRecordLen() {
	int length = 0;
	for (auto col : this->columns) {
		length += col.second.datasize;
	}
	return length;
}
bool sql_tuple::compare(vector<select_predicate> pred,table& thetable) {
	if (pred.size() == 0) {
		return true;
	}
	for (auto pd : pred) {
		if (thetable.columns.count(pd.column_name) == 0) {//未找到指定列
			string errmsg = "Column \"" + pd.column_name + "\" not found.";
			throw errmsg;
		}
		else {
			map<string, column>::iterator here = thetable.columns.find(pd.column_name);
			map<string, column>::iterator it = thetable.columns.begin();
			for (int i = 0; i < thetable.columns.size(); i++) {
				if (it == here) {
					pd.type = it->second.type;
					if (comparator(pd, this->row[i])) {//true
						break;
					}
					else {//false
						return false;
					}
				}
				else {
					it++;
				}

			}
		}
	}
	return true;
}
bool comparator(select_predicate pd, string value) {//单个比较器
	int int_val;
	float float_val;
	string errmsg;
	switch (pd.type) {
	case Int:
		if (!is_int(value)) {
			errmsg = value + " is not a integer";
			throw errmsg;
		}
		else if (!is_int(pd.value)) {
			errmsg = pd.value + " is not a integer";
			throw errmsg;
		}
		else {
			int_val = stoi_h(value);
			switch (pd.op) {
			case eq:
				return int_val == stoi_h(pd.value);
			case ne:
				return int_val != stoi_h(pd.value);
			case ge:
				return int_val >= stoi_h(pd.value);
			case le:
				return int_val <= stoi_h(pd.value);
			case lt:
				return int_val < stoi_h(pd.value);
			case gt:
				return int_val > stoi_h(pd.value);
			}
		}
		break;
	case Float:
		float_val = stof_h(value);
		switch (pd.op) {
		case eq:
			return float_val == stof_h(pd.value);
		case ne:
			return float_val != stof_h(pd.value);
		case ge:
			return float_val >= stof_h(pd.value);
		case le:
			return float_val <= stof_h(pd.value);
		case lt:
			return float_val < stof_h(pd.value);
		case gt:
			return float_val > stof_h(pd.value);
		}
		break;
	default:
		switch (pd.op) {
		case eq:
			return value == pd.value;
		case ne:
			return value != pd.value;
		case ge:
			return value >= pd.value;
		case le:
			return value <= pd.value;
		case lt:
			return value < pd.value;
		case gt:
			return value > pd.value;
		}
	}
	return false;
}

ostream& operator<<(ostream& os, const sql_tuple& thetuple) {
	for (auto tup : thetuple.row) {
		os << setw(OUTW) << tup<<"|";
	}
	return os;
}
ostream& operator<<(ostream& os, const res_set& res) {
	cout << to_string(res.tuples.size()) << " record(s) fetched." << endl;
	for (auto col : res.header) {
		os << setw(OUTW) << col.first << "|";
	}
	os << endl;
	if (res.tuples.size() == 0) {
		for (auto i : res.header) {
			os << setw(OUTW+1) << "NULL|";
		}
		cout << endl;
	}
	for (auto tup : res.tuples) {
		os << tup << endl;
	}
	return os;
}
column::column(string column_define) {
	vector<string> vars=str_tokenizer(column_define," ");
	if (vars.size() != 5) {
		string errmsg = "Incorrect token number, expeting 5 while recieving " + to_string(vars.size()) + ".";
		throw errmsg;
	}
	this->name = vars[0];
	this->type = reverse_type(stoi(vars[1]));
	this->datasize = stoi(vars[2]);
	this->is_pk = stoi(vars[3]) > 0 ? true : false;
	this->is_unique = stoi(vars[4]) > 0 ? true : false;
}
datatype reverse_type(int type) {
	datatype typ;
	switch (type) {
	case Int:return Int;
	case Char:return Char;
	case Float:return Float;

	}
	
}
string column::Cataloginfo() {
	string info = this->name + " " + to_string(this->type) + " " + to_string(this->datasize) + " " + to_string(this->is_pk) + " " + to_string(this->is_unique);
	return info;
}

vector<string> index::Cataloginfo() {
	vector<string> ret_vec;
	ret_vec.push_back("<index>");
	string idxinfo = this->index_name + " " + this->table_name + " " + this->column_name + " "+to_string(this->block_num);
	ret_vec.push_back(idxinfo);
	ret_vec.push_back("</index>");
	return ret_vec;
}



vector<string> table::Cataloginfo() {
	vector<string> ret_vec;
	ret_vec.push_back("<table>");
	string tbl_info = this->tablename + " " + to_string(this->block_num);
	ret_vec.push_back(tbl_info);
	for (auto col : this->columns) {
		ret_vec.push_back(col.second.Cataloginfo());
	}
	ret_vec.push_back("</table>");
	return ret_vec;
}

select_predicate::select_predicate(string predicate) {
	for (size_t pos = 0; pos < predicate.size(); pos++) {//去除空格
		if ((predicate[pos] == ' ' || predicate[pos] == '\t') && !is_in_quotation(predicate, pos)) {//该空格或制表符不在引号内,去除
			predicate.erase(pos,1);
			pos--;
		}
	}
	string col_name;
	if (predicate.find("<>") != string::npos) {
		col_name = str_sub(predicate, "<>");
		this->op = ne;
	}
	else if(predicate.find("<=") != string::npos) {
		col_name = str_sub(predicate, "<=");
		this->op = le;
	}else if(predicate.find(">=") != string::npos) {
		col_name = str_sub(predicate, ">=");
		this->op = ge;
	}
	else if (predicate.find("=") != string::npos) {
		col_name = str_sub(predicate, "=");
		this->op = eq;
	}
	else if (predicate.find("<") != string::npos) {
		col_name = str_sub(predicate, "<");
		this->op = lt;
	}
	else if (predicate.find(">") != string::npos) {
		col_name = str_sub(predicate, ">");
		this->op = gt;
	}
	else {
		string errmsg = "Unknown operator in predicates:" + predicate + ".";
		throw errmsg;
	}


	this->column_name = col_name;
	if (predicate[0] == '\'') {//字符串
		this->type = Char;
		this->value = str_extract(predicate, '\'', '\'');
	}
	else if (predicate[0] == '"') {
		this->type = Char;
		this->value = str_extract(predicate, '"', '"');
	}
	else {
		if (is_int(predicate)) {
			this->type = Int;
			this->value = to_string(stoi_h(predicate));
		}
		else if (is_float(predicate)) {
			this->type = Float;
			this->value = to_string(stof_h(predicate));
		}
	}
}

void table::show_tbl_info() {
	vector<string> d_type_str = { "Int","Char","Float" };
	datatype d;
	cout << setw(15) << "COLUMN NAME|";
	for (auto col : this->columns) {
		cout << setw(15) << col.second.name << "|";
	}
	cout << endl<<"--------------|";
	for (int i = 0; i < this->columns.size(); i++) {
		cout << "---------------|";
	}
	cout << endl;
	cout << setw(15) << "DATA TYPE|";
	for (auto col : this->columns) {
		cout << setw(15) << d_type_str[col.second.type]+"("+to_string(col.second.datasize)+")" << "|";
	}
	cout << endl;
}

void index::print_idxinfo() {
	cout << setw(15) << "INDEX NAME|" << setw(15) << "TABLE NAME|" << setw(15) << "COLUMN NAME|" << endl;
	for (int i = 0; i < 3; i++)
		cout << "--------------|";
	cout << endl;
	cout<< setw(15) << this->index_name+"|" << setw(15) << this->table_name+"|" << setw(15) << this->column_name+"|" << endl;

}


string database::show_tables() {
	string tablenames = "";
	for (auto tbl : tables) {
		tablenames += tbl.second.tablename;
		tablenames += ",";
	}
	return tablenames;
}
void database::show_indices() {
	cout << "There are " + to_string(this->indices.size()) + " indice(s) in the data base. Names are:" << endl;
	for (auto idx : this->indices) {
		cout << idx.second.index_name << endl;
	}
}


vector<int> table::get_PkUnique() {
	int i = 0;
	vector<int> ret_vec;
	for (auto col : this->columns) {
		if (col.second.is_pk || col.second.is_unique) {
			ret_vec.push_back(i);
		}
		i++;
	}
	return ret_vec;
}
string table::get_pk() {
	string name = "";
	for (auto col : this->columns) {
		if (col.second.is_pk) {
			return col.second.name;
		}
	}
	return name;
}
int table::get_pkpos() {
	int i = 0;
	for (auto col : this->columns) {
		if (col.second.is_pk) {
			return i;
		}
		i++;
	}
	return -1;
}
index::index(string catinfo) {

	vector<string> tokens = str_tokenizer(catinfo, " ");
	if (tokens.size() != 4) {
		string errmsg = "Incorrect token number when creating index from catalog file.";
		throw errmsg;
	}
	this->index_name = tokens[0];
	this->column_name = tokens[2];
	this->table_name = tokens[1];
	this->block_num = stoi(tokens[3]);
}

int table::get_col_pos(string col_name) {
	int i = 0;
	for (auto col : this->columns) {
		if (col.second.name == col_name) {
			return i;
		}
		i++;
	}
	return -1;
}

#endif // !MINISQLH