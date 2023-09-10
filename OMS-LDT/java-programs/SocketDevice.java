package edu.lowell.lig.common.dct.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.lowell.lig.common.dct.Activator;
import edu.lowell.lig.common.model.BaseModel;
import edu.lowell.lig.common.model.ErrorLevel;
import edu.lowell.lig.common.model.ErrorMessage;
import edu.lowell.lig.common.utils.ApplicationConstants;
import edu.lowell.lig.jms.core.Messenger;
import edu.lowell.lig.jms.utils.BrokerTopicNames;
import edu.lowell.loui.logging.LogSelector;

public class SocketDevice extends BaseModel {
  /**
   * Logger for this class
   */
  private static final Logger logger = LogManager
      .getLogger(Activator.PLUGIN_ID + LogSelector.getLogger());

  private String name;
  private String address;
  private int port;

  private static final int CONNECT_TIMEOUT = 5 * 1000;
  private int connectionCount;
  private boolean connected;
  private Socket socket;
  private PrintWriter output;
  private BufferedReader input;

  private boolean enabled;

  public SocketDevice() {
    super();
  }

  /**
   * @param name
   * @param address
   * @param port
   */
  public SocketDevice(String name, String address, int port) {
    super();
    this.name = name;
    this.address = address;
    this.port = port;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String host) {
    this.address = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  /**
   * @return the full address including port number
   */
  public String getFullAddress() {
    return getAddress() + ":" + getPort();
  }

  /**
   * @return the display name including the name and full address
   */
  public String getDisplayName() {
    StringBuilder sb = new StringBuilder();
    sb.append(getName());
    sb.append(" (");
    sb.append(getFullAddress());
    sb.append(")");

    return sb.toString();
  }

  public int getConnectionCount() {
    return connectionCount;
  }

  public void setConnectionCount(int connectionCount) {
    this.connectionCount = connectionCount;
  }

  public boolean isConnected() {
    return connected;
  }

  public void setConnected(boolean connected) {
    this.connected = connected;
  }

  public Socket getSocket() {
    return socket;
  }

  public void setSocket(Socket socket) {
    this.socket = socket;
  }

  public PrintWriter getOutput() {
    return output;
  }

  public void setOutput(PrintWriter out) {
    this.output = out;
  }

  public BufferedReader getInput() {
    return input;
  }

  public void setInput(BufferedReader in) {
    this.input = in;
  }

  /**
   * @return the enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @param enabled
   *          the enabled to set
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public void makeConnection() {
    if (logger.isDebugEnabled()) {
      logger.debug("makeConnection(SocketDevice dev=" + this.getDisplayName() +
          ") - start");
    }

    connectionCount++;
    try {
      InetSocketAddress endPoint = new InetSocketAddress(address, port);
      socket = new Socket();
      socket.connect(endPoint, CONNECT_TIMEOUT);
      output = new PrintWriter(socket.getOutputStream(), true);
      input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      connected = true;
      connectionCount = 0;
    } catch (UnknownHostException uhe) {
      logger.error("makeConnection(SocketDevice dev=" + this.getDisplayName() +
          ")", uhe);

      System.exit(1);
    } catch (SocketTimeoutException ste) {
    	// This catch clause added by Dyer Lytle, Aug. 21, 2023
        logger.error("makeConnection(SocketDevice dev=" + this.getDisplayName() +
            ")", ste.getMessage());
        
        List<String> messageList = new ArrayList<String>();
        messageList.add("Error input: " + this.getClass().getName());
        messageList.add("Failed to connect to socket " + getDisplayName());
        messageList.add("Socket connection timed out");
        
        ErrorMessage em = new ErrorMessage(ErrorLevel.SEVERE, this.getClass()
            .getName(), messageList, System.currentTimeMillis());
  
      try {
        Messenger.sendDistributedMessage(BrokerTopicNames.JOE_ERROR,
            em.toXml(), false);
      } catch (Exception e) {
        logger.error("makeConnection()", e); //$NON-NLS-1$
      }
      if (logger.isInfoEnabled()) {
        logger.info("--> Just sent the JOE Error message:" +
            ApplicationConstants.CR + em.toString());
      }
        
    } catch (IOException ioe) {
      logger.error("makeConnection(SocketDevice dev=" + this.getDisplayName() +
          "): " + ioe.getMessage());
      logger.error("Failed to connect: " + new Date() + " - No I/O to " +
          this.getDisplayName());
      logger.error("Number of Connection Attempts: " + connectionCount);
      logger.error("..........Will Try Again Upon Message Reception..........");

      List<String> messageList = new ArrayList<String>();
      messageList.add("Error input: " + this.getClass().getName());
      messageList.add("Failed to connect to socket " + getDisplayName());
      messageList.add("Number of Connection Attempts: " + connectionCount);
      messageList
          .add("..........Will Try Again Upon Message Reception..........");
      ErrorMessage em = new ErrorMessage(ErrorLevel.SEVERE, this.getClass()
          .getName(), messageList, System.currentTimeMillis());
      if (connectionCount == 1) {
        try {
          Messenger.sendDistributedMessage(BrokerTopicNames.JOE_ERROR,
              em.toXml(), false);
        } catch (Exception e) {
          logger.error("makeConnection()", e); //$NON-NLS-1$
        }
        if (logger.isInfoEnabled()) {
          logger.info("--> Just sent the JOE Error message:" +
              ApplicationConstants.CR + em.toString());
        }
      }
    }

    if (logger.isInfoEnabled() && (connectionCount <= 1)) {
      logger.info("--> Opened the connection: " + this);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("makeConnection(SocketDevice) - end");
    }
    return;
  }

  public void terminateConnection() {
    if (socket.isConnected() && !socket.isClosed()) {
      logger.warn("--> Attempting to close the connection: " + this.name);
      try {
        socket.shutdownOutput();
        socket.close();
        connected = false;
      } catch (IOException e) {
        logger.error("terminateConnection()", e); //$NON-NLS-1$
      }
    } else {
      logger.warn("--> Socket is already closed: " + this.name);
    }
  }

}