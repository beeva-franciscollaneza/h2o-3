package water;

import water.util.Log;

/**
 * This thread checks whether the heartbeat from the bully client has been seen before the timeout and if not, it kills
 * the whole h2o cloud
 */
public class ClientHeartBeatCheckThread extends Thread {
    public ClientHeartBeatCheckThread() {
        super("ClientHeartbeatCheckThread");
        setDaemon(true);
    }

    @Override
    public void run(){
        while(true) {
            for (H2ONode n : H2O.getFlatfile()) {
                if (n._heartbeat._bully_client) {
                    if(n._last_heard_from + H2O.ARGS.bully_client_timeout >= System.currentTimeMillis()) {
                        Log.warn("Stopping H2O cloud since bully client hasn't seen heartbeat in specified timeout");
                        H2O.shutdown(0);
                    }else{
                        break;
                    }
                }
            }
            // wait for the duration of timeout
            try {
                sleep(H2O.ARGS.bully_client_timeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
