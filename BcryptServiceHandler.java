import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

//import genJava.BcryptService;
//import genJava.IllegalArgument;

public class BcryptServiceHandler implements BcryptService.Iface {
	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		try {
			if (logRounds > 30) {	// 10 - 30
				throw new IllegalArgument(
						"rounds exceeds maximum (30)");
			}
			List<String> ret = new ArrayList<>();
			for (String onePwd: password) {
				String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
				ret.add(oneHash);
			}
			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		try {
			if (password.size() != hash.size()) {
				throw new IllegalArgument(
						"the length of passwords and hashes does not match");
			}
			List<Boolean> ret = new ArrayList<>();
			for (int i = 0; i < password.size(); ++i) {
				String onePwd = password.get(i);
				String oneHash = hash.get(i);
				ret.add(BCrypt.checkpw(onePwd, oneHash));
			}
			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public boolean connectFE(String hostBE, int portBE) throws IllegalArgument, org.apache.thrift.TException {
		System.out.println("Get connection request from BE node: " + hostBE + ":" + portBE);
		return true;
	}
}
