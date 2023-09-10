package edu.lowell.lig.joe;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.lowell.lig.common.dct.model.OMSCard;
import edu.lowell.lig.common.dct.model.Pdu;
import edu.lowell.lig.common.dct.model.SocketDevice;
import edu.lowell.lig.common.model.ErrorLevel;
import edu.lowell.lig.common.model.ErrorMessage;
import edu.lowell.lig.common.utils.ApplicationConstants;
import edu.lowell.lig.jms.core.Messenger;
import edu.lowell.lig.jms.utils.BrokerTopicNames;
import edu.lowell.lig.jms.utils.JmsUtilities;
import edu.lowell.loui.logging.LogSelector;

public class SocketTester extends Thread {
  /**
   * Logger for this class
   */
  private static final Logger logger = LogManager
      .getLogger(Activator.PLUGIN_ID + LogSelector.getLogger());
  private static final Logger socketTesterLogger = LogManager
      .getLogger(Activator.PLUGIN_ID + ".socketTester" +
          LogSelector.getLogger());

  private static final String PDU_MESSAGE = "DN0\r\n";
  private static final String OMS_MESSAGE = "WY" + ApplicationConstants.CR;
  private SocketDevice dev;
  private boolean messageJustReceived = false;
  private long messageTimestamp = 0;
  private boolean exit;

  public static final int TIME_BETWEEN_ACTIVE_SOCKET_PROBES;

  static {
    XMLConfiguration config = JmsUtilities.getInstrumentConfiguration();
    TIME_BETWEEN_ACTIVE_SOCKET_PROBES = config.getInt(
        "joe.time-between-active-socket-probes",
        ApplicationConstants.DEFAULT_TIME_BETWEEN_ACTIVE_SOCKET_PROBES);
  }

  /**
   * @param dev
   */
  public SocketTester(SocketDevice dev) {
    super();
    this.dev = dev;
    exit = false;
  }

  public void run() {
    if (logger.isTraceEnabled()) {
      logger.trace("run() - start");
    }

    if (logger.isInfoEnabled()) {
      logger.info("-----> Starting socket tester for " +
          this.dev.getFullAddress() + " - Connection Status: " +
          this.dev.isConnected());
    }
    
    for (;;) {
      while (!exit) {  // dlytle added this so can return if interrupted. 090323
      try {
        // This wait/notify technique is preferable to a loop that 
        // repeatedly checks the situation.
        synchronized (this) {
          wait(TIME_BETWEEN_ACTIVE_SOCKET_PROBES);
        }
        if(exit) {
        	return;
        }
        if (!messageJustReceived) {
        	if(exit) {
            	return;
            }
          StringBuilder sb = new StringBuilder();
          if (this.dev instanceof Pdu) {
            sb.append(PDU_MESSAGE);

            StringBuilder command = new StringBuilder();
            command.append(this.dev.getFullAddress());
            command.append(ApplicationConstants.LOIS_DELIMITER);
            command.append(sb);
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_PDU_QUEUE,
                command.toString(), false);
          } else if (this.dev instanceof OMSCard) {
            sb.append(OMS_MESSAGE);
            byte[] bArray = sb.toString().getBytes();
            messageTimestamp = System.currentTimeMillis();
            this.dev.getSocket().getOutputStream().write(bArray);
          }
          if (logger.isInfoEnabled()) {
            socketTesterLogger.info("Socket Test Message: " +
                this.dev.getFullAddress() + "|" + sb.toString().trim());
          }
        }
        messageJustReceived = false;
      } catch (InterruptedException ie) {
        ie.printStackTrace();
//        return;
        break;
      } catch (IOException ioe) {
        // We would like different behavior for the PDU tester and the 
        // OMS tester.
        StringBuilder sb = new StringBuilder();
        sb.append("Error input ").append(this.getClass().getName())
            .append(ApplicationConstants.CR);
        sb.append("I/O Read failed: ").append(this.dev.getDisplayName())
            .append(ApplicationConstants.CR);

        if (this.dev instanceof Pdu) {
          //          // Issue an error warning but do not exit
          //          logger.error("I/O Read failed: run() - " + ioe.getMessage());
          //          logger.info("info on the I/O error above..." +
          //              ApplicationConstants.CR + sb.toString());
          //
          //          String msg = this.dev.getFullAddress() + " " +
          //              ApplicationLabels.PDU_ERROR_MESSAGE;
          //          Messenger.sendDistributedMessage(BrokerTopicNames.JOE_PDU_RESULT_QUEUE,
          //              msg, false);
          //          if (logger.isDebugEnabled()) {
          //            logger.debug(LoggerUtilities.generateBrokerSendLogMessage(
          //                BrokerTopicNames.JOE_PDU_RESULT_QUEUE, msg));
          //          }

          //
          //          Messenger.sendDistributedMessage(BrokerTopicNames.JOE_ERROR,
          //              sb.toString(), false);
          //          if (logger.isInfoEnabled()) {
          //            logger.info("--> Just sent the JOE Error message:" +
          //                ApplicationConstants.CR + sb.toString());
          //          }
        } else if (this.dev instanceof OMSCard) {
        	if(exit) {
            	return;
            }
          // Issue an error warning and exit
          List<String> messageList = new ArrayList<String>();
          messageList.add("Exception Message: " + ioe.getMessage());
          //        messageList.add("***** EXITING JOE *****");
          messageList.add("***** CHECK THE SOCKET CONNECTIONS *****");
          ErrorMessage em = new ErrorMessage(ErrorLevel.SEVERE, this.getClass()
              .getName(), messageList, System.currentTimeMillis());
          try {
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_ERROR,
                em.toXml(), false);
          } catch (Exception e) {
            logger.error("run()", e); //$NON-NLS-1$
          }
          if (logger.isInfoEnabled()) {
            logger.info("--> Just sent the JOE Error message:" +
                ApplicationConstants.CR + em.toString());
          }

          //          System.exit(1);
        }
      }
      }
    }
  }
  
  //for stopping the thread dlytle 090323
  public void stopit()
  {
	  System.out.println("setting exit to true for" + this.dev.getFullAddress());
	  //this.interrupt();
      exit = true;
  }

  //callback, called when any message is received
  synchronized void onMessageReceived() {
    messageJustReceived = true;
    notify();
  }

  public SocketDevice getDev() {
    return dev;
  }

  public long getMessageTimestamp() {
    return messageTimestamp;
  }
}