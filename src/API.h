#pragma once
#include"MiniSQL.h"
#include"Interpreter.h"
#include"BuffferManager.h"
#include"Index.h"
#include"Catalog.h"

buffer buf;
CatalogManager cat;
//语句解释,执行
string sqlhandle(string sqlstmt);

string sqlhandle(string sqlstmt) {
	clock_t clock_s=clock(),clock_e;
	string touser
	string key1, key2;
	string filter = "  ";
	string filtered_sqlstmt = sqlstmt;//为了方便，考虑到合法sql语句关键字部分不会有合法的连续空格，故使用过滤的sqlstmt进行类型判断。但传入构造器的仍然是原语句
	str_filter(filtered_sqlstmt, filter);
	int retv=-3;
	res_set res;
	if (filtered_sqlstmt.empty()) {
		return -2;//检测到空语句
	}
	char delim = ' ';
	//char* tokens = strtok((char*)(filtered_sqlstmt.c_str()), &delim);
	key1 = str_sub(filtered_sqlstmt," ");
	if (key1 == "quit") {
		sql_quit stmt;
		retv = stmt.execute();
	}
	else if (key1 == "execfile") {
		sql_execfile stmt(sqlstmt);
		retv = stmt.execute();
		clock_e = clock();
		float runtime = (clock_e - clock_s) / (float)CLOCKS_PER_SEC;
		cout << "File execution duration:" + to_string(runtime)+" sec(s)."<<endl;
	}
	else {
		//tokens = strtok(NULL, &delim);
		if (key1.empty()) {
			string errmsg = "You have an error in your sql syntax, incomplete sql statement.";
			throw errmsg;
		}
		key2 = str_sub(filtered_sqlstmt," ");
		
		string key = key1 + " " + key2;
		str_tolower(key);
		if (key == "create table") {
			sql_create_table stmt(sqlstmt);
			retv = stmt.execute();
			touser = stmt.resstr;
		}
		else if (key == "drop table") {
			sql_drop_table stmt(sqlstmt);
			retv = stmt.execute();
			touser = stmt.resstr;
		}
		else if (key == "select *") {
			sql_select stmt(sqlstmt);
			res = stmt.execute();
			touser = to_string(res.tuples.size()) + " record(s) fetched." + "/n";
			for (auto col : res.header) {
				touser += col.first + "|";
			}
			touser += "/n";
			if (res.tuples.size() == 0) {
				for (auto i : res.header) {
					touser += "NULL|";
				}
				touser += "/n";
			}
			for (auto tup : res.tuples) {
				touser += tup + "/n";
			}
		}
		else if (key == "create index") {
			sql_create_index stmt(sqlstmt);
			retv = stmt.execute();
			touser = stmt.resstr;
		}
		else if (key == "drop index") {
			sql_drop_index stmt(sqlstmt);
			retv = stmt.execute();
			touser = stmt.resstr;
		}
		else if (key == "insert into") {
			sql_insert stmt(sqlstmt);
			retv = stmt.execute();
			touser = stmt.resstr;
		}
		else if (key == "delete from") {
			sql_delete_from stmt(sqlstmt);
			retv = stmt.execute();
			touser = stmt.resstr;
		}
		else {
			string errmsg = "Unknown sql statement:" + sqlstmt+".";
			throw errmsg;
		}
	}
	
	
	return touser;
}

/*实际执行成员函数*/


int  sql_create_table::execute() {
	if (dbinfo.tables.count(this->table_name) != 0) {
		string errmsg = "Table name \"" + this->table_name + "\" already exist.";
		throw errmsg;
	}
	string file_name = this->table_name + ".table";
	fstream fout(file_name,ios::out);
	if (!fout.is_open()) {
		string errmsg = "Error when creating file:" + file_name + ".";
		throw errmsg;
	}
	fout.close();
	table new_table(this->table_name, this->columns);
	new_table.pk_unique = new_table.get_PkUnique();
	if (!pk_name.empty()) {//为主键建立索引
		
		index newidx;
		newidx.BPT = BuildTreeFromTableString(new_table, this->columns[pk_name].name);
		newidx.column_name = this->columns[pk_name].name;
		newidx.index_name = pk_name + "_idx";
		newidx.table_name = this->table_name;
		dbinfo.indices[newidx.index_name] = newidx;
		//cat.new_index(newidx);
		newidx.BPT = NULL;
	}
	for (auto col : new_table.columns) {
		if (!col.second.is_pk&&col.second.is_unique) {
			new_table.hidden_BPT[col.second.name] = BuildTreeFromTableString(new_table, col.second.name);
		}
	}

	dbinfo.tables[this->table_name] = new_table;
	//cat.new_table(new_table);
	
	resstr = "Table " + this->table_name + " has been successfully created. The columns are:";
	for (auto col : this->columns) {
		resstr += col.second.name + "|";
	}
	

	return 1;
}

int  sql_drop_table::execute() {
	if (dbinfo.tables.count(this->table_name) == 0) {
		string errmsg = "Table name \"" + this->table_name + "\" does not exist.";
		throw errmsg;
	}
	string file_name = this->table_name + ".table";
	table& thetable = dbinfo.tables[this->table_name];
	if (remove(file_name.c_str())!=0) {
		string errmsg = "Error when deleting file:" + file_name + ".";
		throw errmsg;
	}else {
		if (thetable.block_num != 0) {
			insertPos lastrecord = buf.getInsertPosition(thetable);

			for (int blockOffset = 0; blockOffset < thetable.block_num; blockOffset++) {//所有block

				int bufferNum = buf.getIfIsInBuffer(file_name, blockOffset);
				if (bufferNum == -1) {
					bufferNum = buf.getEmptyBuffer();
					buf.readBlock(file_name, blockOffset, bufferNum);
				}
				buf.bufferBlock[bufferNum].initialize();
			}
			thetable.block_num = 0;
		}


		vector<string> idx_to_remove;
		for (auto idx : dbinfo.indices) {
			if (idx.second.table_name == this->table_name) {
				idx_to_remove.push_back(idx.second.index_name);
			}
		}
		for (auto name : idx_to_remove) {
			
			string filename = name + ".idx";
			if (!remove(filename.c_str())) {
				string errmsg = "Error when deleting file:" + filename + ".";
				throw errmsg;
			}
			dbinfo.indices.erase(name);
		}
		dbinfo.tables.erase(this->table_name);
		//cat.updateCat();
	}
	resstr = "Table " + this->table_name + " has been successfully dropped.";
	return 1;
}
int sql_quit::execute() {
	resstr = "Returning to main menu";
	//确保信息更新到了文件中
	for (int i = 0; i < MAXBLOCKNUMBER; i++)
		buf.flashBack(i);
	cat.updateCat();
	for (auto idx : dbinfo.indices) {
		idx.second.savetofile();
	}
	//exit(0);//如果没有错误，则退出
	dbinfo.status = _Start;
	return -1;
}
int sql_create_index::execute() {
	if (dbinfo.tables.count(this->table_name) == 0) {
		string errmsg = "Table \"" + this->table_name + "\" does not exist.";
		throw errmsg;
	}
	if (dbinfo.tables[this->table_name].columns.count(this->column_name) == 0) {
		string errmsg = "Column \"" + this->column_name + "\" does not exist.";
		throw errmsg;
	}
	if (dbinfo.indices.count(this->index_name) != 0) {
		string errmsg = "Index named \"" + this->index_name + "\" already exist.";
		throw errmsg;
	}
	if (dbinfo.tables[this->table_name].columns[this->column_name].is_unique == false) {
		string errmsg = "Column \"" + this->column_name + "\" is not unique.";
		throw errmsg;
	}
	table &thetable = dbinfo.tables[this->table_name];
	index newidx;
	newidx.BPT = BuildTreeFromTableString(thetable, this->column_name);
	newidx.column_name = this->column_name;
	newidx.index_name = this->index_name;
	newidx.table_name = this->table_name;
	dbinfo.indices[newidx.index_name] = newidx;
	//cat.new_index(newidx);
	
	resstr = "Index " + this->index_name + " has been successfully created.";
	return 1;
}
int sql_drop_index::execute() {
	if (dbinfo.indices.count(this->index_name) == 0) {
		string errmsg = "Index named \"" + this->index_name + "\" does not exist.";
		throw errmsg;
	}
	string filename = this->index_name + ".idx";

	if (!remove(filename.c_str()) ) {
		string errmsg = "Error when deleting file:" + filename + ".";
		throw errmsg;
	}
	dbinfo.indices.erase(this->index_name);
	//cat.updateCat();
	resstr = "Index " + this->index_name + " has been successfully dropped." ;
	return 1;
}
res_set sql_select::execute() {
	if (dbinfo.tables.count(this->table_name) == 0) {
		string errmsg = "Table \"" + this->table_name + "\" does not exist.";
		throw errmsg;
	}

	table &thetable = dbinfo.tables[this->table_name];
	string filename = thetable.tablename + ".table";
	if (thetable.block_num == 0) {
		res_set res(thetable);
		return res;
	}
	int length = thetable.getRecordLen() + 1;
	int recordNum = BLOCKSIZE / length;//单个block里最大记录条数
	res_set res(thetable);
	for (int blockOffset = 0; blockOffset < thetable.block_num; blockOffset++) {//所有block
		int bufferNum = buf.getIfIsInBuffer(filename, blockOffset);
		if (bufferNum == -1) {
			bufferNum = buf.getEmptyBuffer();
			buf.readBlock(filename, blockOffset, bufferNum);
		}
		for (int offset = 0; offset < recordNum; offset++) {//block中的每一条记录
			int position = offset * length;
			string stringrow;
			stringrow = buf.bufferBlock[bufferNum].getvalues(position, position + length);

			if (stringrow.c_str()[0] == EMPTY) continue;//inticate that this row of record have been deleted
			stringrow.erase(stringrow.begin());//把第一位去掉
			sql_tuple tmptup(thetable.tablename, stringrow);
			if (tmptup.compare(this->predicates, thetable)) {
				res.tuples.push_back(tmptup);
			}

		}
	}
	//cout << to_string(res.tuples.size()) << " record(s) fetched."<<endl;
	return res;
}

int sql_delete_from::execute() {
	if (dbinfo.tables.count(this->table_name) == 0) {
		string errmsg = "Table \"" + this->table_name + "\" does not exist.";
		throw errmsg;
	}
	int count = 0;
	table &thetable = dbinfo.tables[this->table_name];
	string filename = thetable.tablename + ".table";
	if (thetable.block_num == 0) {
		cout << to_string(count) + " record(s) deleted." << endl;
		return 0;
	}
	if (this->predicates.size() == 0) {//删除所有
		insertPos lastrecord= buf.getInsertPosition(thetable);
		count += (thetable.block_num - 1)*(BLOCKSIZE / (thetable.getRecordLen() + 1));
		count += (lastrecord.position)/(thetable.getRecordLen() + 1);
		for (int blockOffset = 0; blockOffset < thetable.block_num; blockOffset++) {//所有block
			
			int bufferNum = buf.getIfIsInBuffer(filename, blockOffset);
			if (bufferNum == -1) {
				bufferNum = buf.getEmptyBuffer();
				buf.readBlock(filename, blockOffset, bufferNum);
			}
			buf.bufferBlock[bufferNum].initialize();
		}
		thetable.block_num = 0;
		for (auto hidx : thetable.hidden_BPT) {
			thetable.hidden_BPT[hidx.first] = SpawnNewNode();
		}
		for (auto idx : dbinfo.indices) {
			if (idx.second.table_name == thetable.tablename) {
				dbinfo.indices[idx.second.index_name].BPT = SpawnNewNode();
			}
		}
		resstr = to_string(count) + " record(s) deleted." ;
		return 1;
	}

	int length = thetable.getRecordLen() + 1;
	int recordNum = BLOCKSIZE / length;//单个block里最大记录条数
	for (int blockOffset = 0; blockOffset < thetable.block_num; blockOffset++) {//所有block
		int bufferNum = buf.getIfIsInBuffer(filename, blockOffset);
		if (bufferNum == -1) {
			bufferNum = buf.getEmptyBuffer();
			buf.readBlock(filename, blockOffset, bufferNum);
		}
		for (int offset = 0; offset < recordNum; offset++) {//block中的每一条记录
			int position = offset * length;
			string stringrow;
			stringrow = buf.bufferBlock[bufferNum].getvalues(position, position + length);

			if (stringrow.c_str()[0] == EMPTY) continue;//inticate that this row of record have been deleted
			stringrow.erase(stringrow.begin());//把第一位去掉
			sql_tuple tmptup(thetable.tablename, stringrow);
			
			if (tmptup.compare(this->predicates, thetable)) {//所有条件都满足
				string pk_name = thetable.get_pk();
				int pk_pos = thetable.get_pkpos();
				for (auto idx : dbinfo.indices) {
					if (idx.second.table_name == this->table_name) {
						idx.second.deletekey(tmptup.row[thetable.get_col_pos(idx.second.column_name)]);
						dbinfo.indices[idx.second.index_name].deletekey(tmptup.row[thetable.get_col_pos(idx.second.column_name)]);
					}
				}
				int i = 0;
				for (auto col : thetable.columns) {
					if (col.second.is_unique && (!col.second.is_pk)) {
						Remove(thetable.hidden_BPT[col.second.name], (char*)tmptup.row[i].c_str());
					}
					i++;
				}
				buf.bufferBlock[bufferNum].values[position] = DELETED;
				count++;
			}
			buf.bufferBlock[bufferNum].isWritten = 1;
		}

	}
	
	resstr = to_string(count) + " record(s) deleted.";
	return count;
}


int sql_insert::execute() {
	if (dbinfo.tables.count(this->table_name) == 0) {
		string errmsg = "Table \"" + this->table_name + "\" does not exist.";
		throw errmsg;
	}
	int count = 0;
	table &thetable = dbinfo.tables[this->table_name];
	string filename = thetable.tablename + ".table";

	int length = thetable.getRecordLen() + 1;
	int recordNum = BLOCKSIZE / length;//单个block里最大记录条数
	int pk_pos = thetable.get_pkpos();
	string pk_name = thetable.get_pk();

	OffsetType dataoff = dbinfo.indices[pk_name + "_idx"].findkey(this->values[pk_pos].value);
	
	if (dataoff.blockOffset != -1 && dataoff.recordNum != -1) {
		string errmsg = "Unique or primary key column duplicated.";
		throw errmsg;
	}
	for (auto col : thetable.columns) {
		if (col.second.is_unique && (!col.second.is_pk)) {
			OffsetType OFF(-1, -1);
			RecursiveFind(thetable.hidden_BPT[col.second.name], (char*)this->getcoldata(col.second.name).c_str(), 0, NULL, OFF);
			if (OFF.blockOffset != -1 || OFF.recordNum != -1) {
				string errmsg = "Unique or primary key column duplicated.";
				throw errmsg;
			}
		}
	}
	
	if (pk_name.empty()) {//没有主键只能遍历
		for (int blockOffset = 0; blockOffset < thetable.block_num; blockOffset++) {
			int bufferNum = buf.getIfIsInBuffer(filename, blockOffset);
			if (bufferNum == -1) {
				bufferNum = buf.getEmptyBuffer();
				buf.readBlock(filename, blockOffset, bufferNum);
			}
			for (int offset = 0; offset < recordNum; offset++) {//block中的每一条记录
				int position = offset * length;
				string stringrow;
				stringrow = buf.bufferBlock[bufferNum].getvalues(position, position + length);

				if (stringrow.c_str()[0] == EMPTY) continue;//inticate that this row of record have been deleted
				stringrow.erase(stringrow.begin());//把第一位去掉
				sql_tuple tmptup(thetable.tablename, stringrow);
				for (auto no : thetable.pk_unique) {
					if (this->values[no].value == tmptup.row[no]) {//主键或独一列重复
						string errmsg = "Unique or primary key column duplicated.";
						throw errmsg;
					}
				}



			}
		}
	}
	
		/*
		map<string, column>::iterator it = thetable.columns.begin();
		for (int i = 0; i < thetable.columns.size(); i++) {//检验unique和primary key是否重复
			if (it->second.is_pk || it->second.is_unique) {
				string stmt = "select * from " + thetable.tablename + " where " + it->second.name + "=" + this->values[i].value;
				sql_select sqlstmt(stmt);
				res_set res = sqlstmt.execute();
				if (res.tuples.size() > 0) {
					string errmsg = "Unique or primary key column duplicated. Name:\"" + it->second.name + "\"";
					throw errmsg;
				}
			}
		}
		*/
		string insert_val = this->connect();
		if (insert_val.size() != thetable.getRecordLen()) {
			string errmsg = "Incorrect record length, recieved " + to_string(insert_val.size()) + " expecting " + to_string(thetable.getRecordLen()) + ".";
			throw errmsg;
		}
		//一切ok，进行插入
		insertPos iPos = buf.getInsertPosition(thetable);
		buf.bufferBlock[iPos.bufferNUM].values[iPos.position] = NOTEMPTY;
		for (int i = 0; i < thetable.getRecordLen(); i++) {
			buf.bufferBlock[iPos.bufferNUM].values[iPos.position + i + 1] = insert_val.c_str()[i];
		}
		buf.bufferBlock[iPos.bufferNUM].isWritten = true;
		if (!pk_name.empty()) {//主键非空，插入key值到索引
			index& theidx = dbinfo.indices[pk_name + "_idx"];
			OffsetType Offset(thetable.block_num-1, iPos.position / (thetable.getRecordLen() + 1));
			theidx.BPT= Insert(theidx.BPT, (char*)this->values[pk_pos].value.c_str(), Offset);
			for (auto col : thetable.columns) {
				if (col.second.is_unique && (!col.second.is_pk)) {
					thetable.hidden_BPT[col.second.name] = Insert(thetable.hidden_BPT[col.second.name], (char*)this->getcoldata(col.second.name).c_str(), Offset);

				}
			}
		}
		//string out = "Successfully insert a record to " + this->table_name + ".";
		//printf("%s", out.c_str());
		resstr = "Successfully insert a record to " + this->table_name + ".";
		return 1;
	}

int sql_execfile::execute() {
	fstream fin(this->filename);
	if (!fin.is_open()) {
		string errmsg = "Error when loading file:" + this->filename;
		throw errmsg;
	}
	string buffer = "";
	while (!fin.eof()) {
		string line;
		getline(fin, line);
		size_t last_pos = 0;
		while (line.find(';', last_pos) != string::npos) {//找到分号
			size_t pos = line.find_first_of(';', last_pos);
			if (!is_in_quotation(line, pos)) {//第一个分号不在括号内
				string addstr = str_sub(line, pos);
				buffer += " " + addstr;
				str_filter(buffer, ' ');
				//cout << buffer << endl;
				try {
					int ret_v = sqlhandle(buffer);
					if (ret_v == -1)//有退出命令
						return -1;
				}
				catch (string e) {
					cout << e << endl;
				}
				
				buffer.clear();
			}
			else {//第一个分号在括号内
				last_pos = pos + 1;
			}
		}
		buffer += " " + line;
	}
	return 1;
}