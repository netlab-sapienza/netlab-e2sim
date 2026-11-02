#include "bs_connector.hpp"
#include "encode_e2apv1.hpp"
#include "encode_kpm.hpp"
#include "kpm_callbacks.hpp"
#include "indication_sequence.hpp"
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <boost/asio.hpp>
#include <atomic>
#include <mutex>
#include <vector>

extern "C" {
  #include "csv_reader.h"
  #include "E2SM-KPM-IndicationMessage.h"
  #include "E2SM-KPM-RANfunction-Description.h"
  #include "E2SM-KPM-IndicationHeader-Format1.h"
  #include "E2SM-KPM-IndicationHeader.h"
  #include "Timestamp.h"
  #include "E2AP-PDU.h"
}

static std::atomic<bool> report_data_nrt_ric{true};
static std::atomic<bool> protobuf_reporter_active{false};
static std::mutex protobuf_request_mutex;
static std::vector<uint8_t> protobuf_request;
static long protobuf_requestor_id = 0;
static long protobuf_instance_id = 0;
static long protobuf_function_id = 0;
static long protobuf_action_id = 0;

// handle timer got from RIC Subscription Request
// timer is in seconds
void handleTimer(E2Sim* e2sim, int* timer, long* ric_req_id, long* ric_instance_id,
  long* ran_function_id, long* action_id, uint8_t *indreq_buff, int indreq_buflen) {

  int seq_num = 1;

  fprintf(stderr, "Handle timer %d seconds, ricReqId %ld\n", timer[0], ric_req_id[0]);

  // // populate thread arguments
  // thread_args *t_args = (thread_args*) calloc(1, sizeof(thread_args));
  // t_args->timer = timer;
  // t_args->ric_req_id = ric_req_id;

  // start thread
  report_data_nrt_ric = 1;

  if (OAI_PROTOBUF) {
    // A reconnect may produce a new subscription while the old reporter still
    // owns UDP port 6600. Refresh its request instead of binding a second socket.
    {
      std::lock_guard<std::mutex> lock(protobuf_request_mutex);
      protobuf_request.assign(indreq_buff, indreq_buff + indreq_buflen);
      protobuf_requestor_id = ric_req_id[0];
      protobuf_instance_id = ric_instance_id[0];
      protobuf_function_id = ran_function_id[0];
      protobuf_action_id = action_id[0];
    }
    bool expected = false;
    if (!protobuf_reporter_active.compare_exchange_strong(expected, true)) {
      fprintf(stderr, "Updating existing protobuf reporter subscription; no second UDP bind\n");
      return;
    }
    try {
      std::thread(periodicDataReportOaiProtobuf, e2sim, timer, seq_num, ric_req_id, ric_instance_id,
        ran_function_id, action_id).detach();
    } catch (...) {
      protobuf_reporter_active = false;
      throw;
    }
    fprintf(stderr, "periodicDataReportOaiProtobuf thread created successfully\n");
  }
  else {
    std::thread(periodicDataReport, e2sim, timer, seq_num, ric_req_id, ric_instance_id, 
      ran_function_id, action_id).detach();
    fprintf(stderr, "periodicDataReport thread created successfully\n");
  }
}


// function to periodically report data
void periodicDataReport(E2Sim* e2sim, int* timer, long seqNum, long* ric_req_id, long* ric_instance_id,
  long* ran_function_id, long* action_id) {

  long requestorId = ric_req_id[0];
  long instanceId = ric_instance_id[0];
  long ranFunctionId = ran_function_id[0];
  long actionId = action_id[0];

  fprintf(stderr, "timer expired for requestorId %ld, instanceId %ld, ranFunctionId %ld, actionId %ld: %d s\n",
    requestorId, instanceId, ranFunctionId, actionId, timer[0]);

  char* payload = NULL;
  // E2AP_PDU *e2ap_pdu = (E2AP_PDU*)calloc(1,sizeof(E2AP_PDU));
  
  if (DEBUG) {
    fprintf(stderr, "DEBUG mode\n");
    payload = (char*) "{\"timestamp\":1602706183796,\"slice_id\":0,\"dl_bytes\":53431,\"dl_thr_mbps\":2.39,\"ratio_granted_req_prb\":0.02,\"slice_prb\":6,\"dl_pkts\":200}";
  }
  else {
    get_tx_string(&payload, LINES_TO_READ);
  }

  // send PDU
  if (payload) {
    fprintf(stderr, "Sending\n%s\n", payload);
    fprintf(stderr, "Encoding RIC Indication Report\n");
    // encoding::generate_e2apv1_indication_report(e2ap_pdu, payload, strlen(payload), ric_req_id[0], 0, 0, 0);
    // fprintf(stderr, "RIC Indication Report successfully encoded\n");
    // e2sim->encode_and_send_sctp_data(e2ap_pdu);

    // ASN.1 encode payload and header
    encode_and_send_ric_indication_report_metrics_buffer(payload, strlen(payload), seqNum, requestorId, instanceId, ranFunctionId, actionId);
    seqNum = next_indication_sequence(seqNum);
  }
  
  std::chrono::seconds sleep_duration(timer[0]);
  std::this_thread::sleep_for(sleep_duration);

  // loop thread
  if (report_data_nrt_ric) {
    periodicDataReport(e2sim, timer, seqNum, ric_req_id, ric_instance_id, ran_function_id, action_id);
  }
}


// function to periodically report data
void periodicDataReportOaiProtobuf(E2Sim *e2sim, int *timer, long seqNum, long *ric_req_id, long *ric_instance_id,
                        long *ran_function_id, long *action_id)
{
  (void)e2sim;
  (void)ric_req_id;
  (void)ric_instance_id;
  (void)ran_function_id;
  (void)action_id;
  //char *payload = NULL;
  // E2AP_PDU *e2ap_pdu = (E2AP_PDU*)calloc(1,sizeof(E2AP_PDU));

  int out_port = 6655;
  int in_port = 6600;
  boost::asio::io_service io_service;
  std::chrono::milliseconds sleep_duration(500);

  // out socket (to gnb)
  boost::asio::ip::udp::socket out_socket(io_service);
  boost::asio::ip::udp::endpoint remote_endpoint_out;
  out_socket.open(boost::asio::ip::udp::v4());
  remote_endpoint_out = boost::asio::ip::udp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), out_port);

  // in socket (from gnb)
  boost::asio::ip::udp::socket in_socket(io_service);
  boost::asio::ip::udp::endpoint remote_endpoint_in;
  in_socket.open(boost::asio::ip::udp::v4());
  remote_endpoint_in = boost::asio::ip::udp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), in_port);
  boost::system::error_code bind_error;
  in_socket.bind(remote_endpoint_in, bind_error);
  if (bind_error) {
    fprintf(stderr, "KPM reporter cannot bind UDP port %d: %s\n", in_port,
            bind_error.message().c_str());
    protobuf_reporter_active = false;
    return;
  }
  //in_socket.bind(remote_endpoint_in, in_port);
  boost::system::error_code err;

  char recvbuf[4097];
  size_t recvlen;

  E2AP_PDU *e2ap_pdu = NULL;

  while (report_data_nrt_ric)
  {
    long requestorId, instanceId, ranFunctionId, actionId;
    std::vector<uint8_t> request;
    {
      std::lock_guard<std::mutex> lock(protobuf_request_mutex);
      request = protobuf_request;
      requestorId = protobuf_requestor_id;
      instanceId = protobuf_instance_id;
      ranFunctionId = protobuf_function_id;
      actionId = protobuf_action_id;
    }

    fprintf(stderr, "timer expired for requestorId %ld, instanceId %ld, ranFunctionId %ld, actionId %ld: %d s\n",
            requestorId, instanceId, ranFunctionId, actionId, timer[0]);
    fprintf(stderr, "Sending indication request buffer to gnb, size %zu\n", request.size());
    
    fprintf(stderr,"about to print buffer in periodicDataReportOaiProtobuf\n");
    for(size_t i = 0; i < request.size(); i++){
      fprintf(stderr,"---%hhx\n", request[i]);
    }
    fprintf(stderr, "\n");
    
    // payload = (char*) "{\"timestamp\":1602706183796,\"slice_id\":0,\"dl_bytes\":53431,\"dl_thr_mbps\":2.39,\"ratio_granted_req_prb\":0.02,\"slice_prb\":6,\"dl_pkts\":200}";
    out_socket.send_to(boost::asio::buffer(request), remote_endpoint_out, 0, err);

    fprintf(stderr, "Waiting for response from gnb...");
    std::this_thread::sleep_for(sleep_duration);
    recvlen = in_socket.receive_from(boost::asio::buffer(recvbuf, sizeof(recvbuf) - 1), remote_endpoint_in);
    fprintf(stderr, " recevied %lu bytes\n", recvlen);

    // append null terminator to payload such that the buffer can be processed by the kind-of-stupid following encoders
    recvbuf[recvlen++] = '\0';

    fprintf(stderr,"printing buf recevied from gnb:\n");
    for(int i = 0; i < recvlen; i++){
      fprintf(stderr, " %hhx ", recvbuf[i]);
    }
    fprintf(stderr, "\n");

    fprintf(stderr,"Sending report to ric\n");

    // send PDU
    if (recvlen > 0) {
      // fprintf(stderr, "Sending\n%s\n", recvbuf);
      fprintf(stderr, "Encoding RIC Indication Report\n");
      // encoding::generate_e2apv1_indication_report(e2ap_pdu, recvbuf, strlen(recvbuf), ric_req_id[0], 0, 0, 0);
      // fprintf(stderr, "RIC Indication Report successfully encoded\n");
      // e2sim->encode_and_send_sctp_data(e2ap_pdu);

      // ASN.1 encode payload and header
      encode_and_send_ric_indication_report_metrics_buffer(recvbuf, recvlen, seqNum, requestorId, instanceId, ranFunctionId, actionId);
      seqNum = next_indication_sequence(seqNum);
    }
  }
  protobuf_reporter_active = false;
}


// log message on file
void log_message(char* message, char* message_type, int len) {

  FILE *fp;
  char filename[100] = "/logs/du_l2.log";

  char buffer[26];
  int millisec;
  struct tm* tm_info;
  struct timeval tv;

  gettimeofday(&tv, NULL);

  millisec = lrint(tv.tv_usec/1000.0);  // Round to nearest millisec
  if (millisec>=1000) {  // Allow for rounding up to nearest second
    millisec -=1000;
    tv.tv_sec++;
  }

  tm_info = localtime(&tv.tv_sec);

  strftime(buffer, 26, "%Y:%m:%d %H:%M:%S", tm_info);

  fp = fopen(filename, "a+");

  if (fp == NULL) {
    printf("ERROR: fp is NULL\n");
    return;
  }

  const int msg_len = len;
  char msg_copy[msg_len];
  strcpy(msg_copy, message);

  for (int i = 0; i < msg_len; i++)
  {
    if (message[i] == '\n') {
       msg_copy[i] = 'n';
    }
  }

  // print to console and log on file
  printf("%s,%03d\t%s\t%d\t%s\n", buffer, millisec, message_type, len, msg_copy);
  fprintf(fp, "%s,%03d\t%s\t%d\t%s\n", buffer, millisec, message_type, len, msg_copy);

  fclose(fp);
}


// terminate periodic thread that reports data to near real-time RIC
void stop_data_reporting_nrt_ric(void) {
  printf("Terminating data reporting to near-real-time RIC\n");
  report_data_nrt_ric = 0;
}

// send udp datagrams containing buffers to gnb (was send_ricindi_to_bs)
void send_msg_buffer_to_bs(uint8_t *buffer, int buflen) {
  fprintf(stderr, "sending udp datagram\n");
  boost::asio::io_service io_service;
  boost::asio::ip::udp::socket socket(io_service);
  boost::asio::ip::udp::endpoint remote_endpoint;
  socket.open(boost::asio::ip::udp::v4());
  int out_port = 6655;
  remote_endpoint = boost::asio::ip::udp::endpoint(boost::asio::ip::address::from_string("127.0.0.1"), out_port);
  boost::system::error_code err;
  socket.send_to(boost::asio::buffer(buffer, buflen), remote_endpoint, 0, err);
}
