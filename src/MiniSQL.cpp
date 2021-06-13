#include"MiniSQL.h"
#include"API.h"
extern buffer buf;
//处理用户输入
void handle_raw_input();
//初始欢迎界面
void welcome();
//各层级界面
void interface();
//主界面用户选择处理
int main_usr_handle(string input);
//系统命令处理，要求整行输入
int command_handle(string str);
//帮助显示，用于系统命令
void print_help();
int sockfd1, newfd1;
void* deal_socket();

int main() {
	int ret;
	pthread_t read_tid, write_tid;
	struct sockaddr_in SQL_addr;
	SQL_addr.sin_family = AF_INET;			    // 设置域为IPV4
	SQL_addr.sin_addr.s_addr = INADDR_ANY;		// 绑定到 INADDR_ANY 地址
	SQL_addr.sin_port = htons(3691);			    // 端口转换
	sockfd1 = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd1 < 0)
	{
		exit(1);								// 调用socket函数建立socket描述符出错
	}
	ret1 = bind(sockfd1, (struct sockaddr*)(&SQL_addr), sizeof(SQL_addr));
	perror("SQL");
	if (ret1 < 0) {
		exit(2);
	}
	ret1 = listen(sockfd1, 4);
	if (ret1 < 0) {
		exit(3);
	}
	newfd1 = accept(sockfd1, NULL, NULL);
	if (newfd1 < 0) {
		exit(4);
	}
	string tablenames;
	tablenames = show_tables();
	send(newfd1, tablenames, sizeof(tablenames), 0);

	pthread_create(&read_tid, NULL, deal_socket, NULL);
	while (1) {
		sleep(10000);
	}
}

void* deal_socket(){
	int recv_num, restlt_num;
	string touser;
	string recv_buf[511];
	while (1) {
		memset(recv_buf, 0, sizeof(recv_buf));     //清空recv_buf缓存区
		recv_num = recv(newfd1, recv_buf, 255, 0);
		if (recv_num > 0) {
			touser = sqlhandle(recv_buf);
			send(newfd1, touser, sizeof(touser), 0);
		}
	}

}

void interface() {
	switch (dbinfo.status) {
	case _Start:
		cout << "Welcome to minisql."<<endl;
		cout << "----------MENU----------" << endl;
		cout << "[1]System commad" << endl;
		cout << "[2]Sql" << endl;
		cout << "[3]Exit" << endl;
		break;
	case _SQL:
		cout << "Now is in sql mode." << endl;
		break;
	case _Command:
		cout << "Now is in command mode, press /? for help."<<endl;
		break;
	}
}

void handle_raw_input() {
	string buffer = "";
	while (1) {
		string line;
		getline(cin, line);
		size_t last_pos = 0;
		while (line.find(';', last_pos) != string::npos) {//找到分号
			size_t pos = line.find_first_of(';', last_pos);
			if (!is_in_quotation(line, pos)) {//第一个分号不在括号内
				string addstr = str_sub(line, pos);
				buffer += " " + addstr;
				str_filter(buffer, ' ');
				//cout << buffer << endl;
				try {
					int ret_v=sqlhandle(buffer);
					if (ret_v == -1) {
						return;
					}
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
}

void welcome() {
	cout << "----------------WELCOME---------------"<<endl;
}
int main_usr_handle(string input) {
	if (input.empty()) {
		return -1;//无效输入
	}
	if (input == "1") {
		dbinfo.status = _Command;
		return 1;
	}
	if (input == "2") {
		dbinfo.status = _SQL;
		return 2;
	}if (input == "3") {
		cout << "Good bye" << endl;
		exit(0);

	}
}
int command_handle(string str) {
	   /*目前支持的系统命令
	   /? 显示帮助
	   /showtbl [tablename] 显示表的定义信息
	   /showidx [indexname] 显示索引信息
	   /quit 返回主菜单
	   /sql sql模式
	   */
	if (str.empty() || str[0] != '/') {
		print_help();
		return 0;
		}
	if (str == "/quit") {
		dbinfo.status = _Start;
		return -1;
	}
	else if (str == "/sql") {
		dbinfo.status = _SQL;
		return -1;
	}
	vector<string> tokens = str_tokenizer(str, " ");
	
	if (tokens[0] == "/showtbl") {
		if (tokens.size() > 2) {
			cout << "/showtbl [tablename] //显示表的定义信息" << endl;
			return 0;
		}
		if (tokens.size() == 1) {
			dbinfo.show_tables();
			return 1;
		}
		if (dbinfo.tables.count(tokens[1]) == 0) {
			cout << "Table named \"" + tokens[1] + "\" not found."<<endl;
			return 0;
		}
		table& thetable = dbinfo.tables[tokens[1]];
		thetable.show_tbl_info();
		return 1;
	}
	else if (tokens[0] == "/showidx") {
		if (tokens.size() > 2) {
			cout << "/showidx [indexname] //显示索引信息." << endl;
			return 0;
		}
		if (tokens.size() == 1) {
			dbinfo.show_indices();
			return 1;
		}
		if (dbinfo.indices.count(tokens[1]) == 0) {
			cout << "Index named \"" + tokens[1] + "\" not found." << endl;
			return 0;
		}
		else {
			dbinfo.indices[tokens[1]].print_idxinfo();
		}
		
		return 1;
	}
	else {//未知指令
		print_help();
		return 0;
	}
}

void print_help() {
	cout << "Supported commands until now:" << endl;
	cout << "/?	                  //show this help." << endl;
	cout << "/showtbl [tablename] //Show table definitions." << endl;
	cout << "/showtbl             //Show all table names." << endl;
	cout << "/showidx [indexname] //show index information." << endl;
	cout << "/showidx             //show all index names ." << endl;
	cout << "/quit                //return to main menu." << endl;
	cout << "/sql                 //switch to sql mode." << endl;
	return;
}