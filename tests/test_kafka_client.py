import unittest
from unittest.mock import MagicMock, patch, ANY
from src.infrastructure.kafka_client import KafkaMessagingService

class TestKafkaMessagingService(unittest.TestCase):

    @patch('src.infrastructure.kafka_client.KafkaProducer')
    def setUp(self, mock_producer_class):
        """
        mock_producer_class — це і є наш Mock, який замінив клас KafkaProducer.
        """
        self.mock_producer_class = mock_producer_class
        self.mock_producer_instance = mock_producer_class.return_value
        self.bootstrap_servers = "localhost:9092"
        self.service = KafkaMessagingService(self.bootstrap_servers)

    def test_init_connection(self):
        """Перевіряємо, чи правильно ініціалізується продюсер."""
        # ТУТ ПОМИЛКА БУЛА: треба звертатися до self.mock_producer_class, 
        # а не до імпортованого типу KafkaProducer
        self.mock_producer_class.assert_called_with(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=ANY,
            retries=5,
            retry_backoff_ms=1000
        )

    def test_send_message_success(self):
        """Перевіряємо логіку відправки повідомлення."""
        topic = "test_topic"
        message = {"id": 1, "text": "hello"}
        
        self.service.send_message(topic, message)
        
        self.mock_producer_instance.send.assert_called_once_with(
            topic, value=message
        )

    def test_send_message_exception_handling(self):
        """Перевіряємо, чи не 'падає' сервіс, якщо Kafka видає помилку."""
        # Симулюємо помилку
        self.mock_producer_instance.send.side_effect = Exception("Kafka Down")
        
        try:
            self.service.send_message("topic", {"data": "test"})
            execution_failed = False
        except Exception:
            execution_failed = True
            
        self.assertFalse(execution_failed, "Service should handle exception internally")

if __name__ == '__main__':
    unittest.main()