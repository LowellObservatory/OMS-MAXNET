package edu.lowell.lig.joe;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.lowell.lig.common.dct.model.OMSCard;
import edu.lowell.lig.common.model.ErrorLevel;
import edu.lowell.lig.common.model.ErrorMessage;
import edu.lowell.lig.common.utils.ApplicationConstants;
import edu.lowell.lig.common.utils.LoggerUtilities;
import edu.lowell.lig.jms.core.Messenger;
import edu.lowell.lig.jms.utils.BrokerTopicNames;
import edu.lowell.lig.jms.utils.JmsUtilities;
import edu.lowell.loui.logging.LogSelector;

public class OMSSocketRouter extends Thread {

  /**
   * Logger for this class
   */
  private static final Logger logger = LogManager
      .getLogger(Activator.PLUGIN_ID + LogSelector.getLogger());
  private static final Logger socketTesterLogger = LogManager
      .getLogger(Activator.PLUGIN_ID + ".socketTester" +
          LogSelector.getLogger());
  private static final Logger socketReadoutLogger = LogManager
      .getLogger(Activator.PLUGIN_ID + ".socketReadout" +
          LogSelector.getLogger());

  private boolean errorMessageSent = false;
  private int numErrors = 0;

  static boolean allDone = false;
  private OMSCard card;
  private SocketTester socketTester;

  private static final int TIME_BETWEEN_ACTIVE_SOCKET_PROBES;
  private static final int SOCKET_PROBES_FREQUENCY_TIMEOUT;
  //private static final int MAX_NUM_ERRORS = 1000;
  private static final int MAX_NUM_ERRORS = 25;  // Lytle changed to 25 on 090323

  InputStream istream;
  BufferedReader input;
  
  static {
    XMLConfiguration config = JmsUtilities.getInstrumentConfiguration();
    TIME_BETWEEN_ACTIVE_SOCKET_PROBES = config.getInt(
        "joe.time-between-active-socket-probes",
        ApplicationConstants.DEFAULT_TIME_BETWEEN_ACTIVE_SOCKET_PROBES);
    SOCKET_PROBES_FREQUENCY_TIMEOUT = config.getInt(
        "joe.socket-probes-frequency-timeout",
        ApplicationConstants.DEFAULT_SOCKET_PROBES_FREQUENCY_TIMEOUT);
  }

  /**
   * @param card
   */
  public OMSSocketRouter(OMSCard card) {
    super();
    this.card = card;
    this.socketTester = new SocketTester(card);
  }

  public void run() {
    if (logger.isTraceEnabled()) {
      logger.trace("run() - start");
    }

    if (logger.isInfoEnabled()) {
      logger.info("-----> Starting OMS socket router for " +
          this.card.getFullAddress() + " - Connection Status: " +
          this.card.isConnected());
    }

    socketTester.start();
    
    while (!this.isInterrupted()) {  // dlytle added this so can return if interrupted. 090323
    	
    try {
      this.card.getSocket().setSoTimeout(
          TIME_BETWEEN_ACTIVE_SOCKET_PROBES * SOCKET_PROBES_FREQUENCY_TIMEOUT);
    } catch (SocketException e) {
      logger.error("run()", e);
    }

    String line;
    long lineNumber = 0;

    while (this.card.isConnected()) {
      try {
        line = this.card.getInput().readLine();
        if ((line != null) && (line.trim().length() != 0)) {
          if (logger.isInfoEnabled()) {
            String logMessage = lineNumber++ + "|" +
                this.card.getFullAddress() + "|" + line + "|";
            if (line.contains(ApplicationConstants.OMS_CARD_IDENTIFICATION)) {
              socketTesterLogger.info(logMessage);
            } else {
              socketReadoutLogger.info(logMessage);
            }
          }
          StringBuilder sb = new StringBuilder();
          sb.append(this.card.getFullAddress()).append(" ").append(line);
          Messenger.sendDistributedMessage(
              BrokerTopicNames.JOE_STAGE_RESULT_QUEUE, sb.toString(), false);
          if (logger.isTraceEnabled()) {
            logger.trace(LoggerUtilities.generateBrokerSendLogMessage(
                BrokerTopicNames.JOE_STAGE_RESULT_QUEUE, sb.toString()));
          }

          socketTester.onMessageReceived();
        }
        if (line == null) {
          String msg = "byteRead = -1, Connection seems to be closed: " +
              this.card.getDisplayName();
          logger.error(msg);
          throw new IOException(msg);
        }
      } catch (IOException ioe) {
        logger.fatal("I/O Read failed: run()", ioe);

        if (!errorMessageSent) {
          errorMessageSent = true;
          numErrors++;
          List<String> messageList = new ArrayList<String>();
          messageList.add("Exception Message: " + ioe.getMessage());
          messageList.add("I/O Read failed: " + this.card.getDisplayName());
          //        messageList.add("***** EXITING JOE *****");
          messageList.add("***** CHECK THE SOCKET CONNECTIONS *****");
          messageList.add("Number of Errors: " + numErrors);
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
        } else {
          numErrors++;
        }

        if ((numErrors % MAX_NUM_ERRORS) == 0) {
          // terminate the connection
          this.card.terminateConnection();
          // let's also interrupt our socket tester and return from this thread. Dlytle 090323
          if (logger.isInfoEnabled()) {
              logger.info("Terminating the socket tester and socket router for " + this.card.getDisplayName());
            }
          socketTester.getDev().terminateConnection();
          socketTester.stopit();
          return;
        } else {
          // Too many errors. Let's send the next error message
          errorMessageSent = false;
          // System.exit(1);
        }
      }
      if (allDone) {
        if (logger.isInfoEnabled()) {
          logger.info("We are all done with OMSSocketRouter thread.");
        }

        if (logger.isTraceEnabled()) {
          logger.trace("run() - end");
        }
        return;
      }
    }
    } // dlytle 090323
  }

  public OMSCard getCard() {
    return card;
  }
  
  // Lytle added following routine 090223.
  public SocketTester getSocketTester() {
    return this.socketTester;
  }

}