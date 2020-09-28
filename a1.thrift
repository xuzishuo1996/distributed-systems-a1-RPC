exception IllegalArgument {
  1: string message;
}

service BcryptService {
 list<string> hashPassword (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> checkPassword (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
}

service ConnectFEService {
 bool connectFE (1: string hostBE, 2: i32 portBE) throws (1: IllegalArgument e);
}
