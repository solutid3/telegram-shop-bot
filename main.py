"""
TELEGRAM SHOP BOT v2.0 - ПРОФЕССИОНАЛЬНАЯ СИСТЕМА АВТОПРОДАЖ
Автоматизация цифровых продаж с AI-ассистентом
"""

import asyncio
import logging
import json
import hashlib
import datetime
import uuid
from decimal import Decimal
from typing import Dict, List, Optional, Any
from enum import Enum
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, WebAppInfo, LabeledPrice,
    PreCheckoutQuery, SuccessfulPayment, ShippingQuery,
    InputFile, FSInputFile, URLInputFile
)
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.utils.markdown import hbold, hlink, hcode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
import aiohttp
import redis.asyncio as redis
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, JSON, DateTime, Text, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import qrcode
from io import BytesIO
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from cryptography.fernet import Fernet
import stripe
import yookassa

# ==================== КОНФИГУРАЦИЯ ====================
class Config:
    """Конфигурация бота"""
    
    # Telegram
    BOT_TOKEN = ""
    ADMIN_IDS = []  # ID администраторов
    SUPPORT_CHAT_ID = -  # Чат техподдержки
    
    # Базы данных
    REDIS_URL = "redis://localhost:6379/0"
    DATABASE_URL = "sqlite:///shop_bot.db"  # Или PostgreSQL
    
    # Платежные системы
    YOOKASSA_SHOP_ID = "your_shop_id"
    YOOKASSA_SECRET_KEY = "your_secret_key"
    CRYPTOBOT_TOKEN = "your_cryptobot_token"
    STRIPE_API_KEY = "your_stripe_key"
    
    # WebApp
    WEBAPP_URL = "https://ваш-домен.рф/webapp"
    WEBHOOK_URL = "https://ваш-домен.рф/webhook"
    WEBHOOK_PATH = "/webhook"
    
    # Настройки
    REFERRAL_PERCENT = 15  # Процент от покупки рефереру
    REFERRAL_LEVELS = 3    # Уровни реферальной системы
    MIN_WITHDRAW = 500     # Минимальная сумма вывода
    SUPPORT_RATE_LIMIT = 5 # Сообщений в минуту
    
    # Кэш
    CACHE_TTL = 3600  # Время жизни кэша в секундах

# ==================== БАЗА ДАННЫХ ====================
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, nullable=False)
    username = Column(String(255))
    first_name = Column(String(255))
    last_name = Column(String(255))
    language_code = Column(String(10))
    balance = Column(Float, default=0.0)
    total_spent = Column(Float, default=0.0)
    total_earned = Column(Float, default=0.0)
    referral_code = Column(String(50), unique=True)
    referred_by = Column(BigInteger)  # user_id того, кто пригласил
    registration_date = Column(DateTime, default=datetime.datetime.utcnow)
    last_activity = Column(DateTime, default=datetime.datetime.utcnow)
    is_banned = Column(Boolean, default=False)
    is_premium = Column(Boolean, default=False)
    settings = Column(JSON, default={})
    
    # Статистика
    orders_count = Column(Integer, default=0)
    messages_count = Column(Integer, default=0)
    successful_refs = Column(Integer, default=0)

class Product(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    price = Column(Float, nullable=False)
    category = Column(String(100))
    subcategory = Column(String(100))
    image_url = Column(String(500))
    file_url = Column(String(500))  # Для цифровых товаров
    file_password = Column(String(100))  # Пароль на архив (если нужно)
    stock = Column(Integer, default=-1)  # -1 = бесконечный
    is_active = Column(Boolean, default=True)
    is_hot = Column(Boolean, default=False)
    is_new = Column(Boolean, default=True)
    tags = Column(JSON, default=[])
    attributes = Column(JSON, default={})  # Доп. атрибуты
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Продажи
    sales_count = Column(Integer, default=0)
    total_revenue = Column(Float, default=0.0)
    rating = Column(Float, default=5.0)
    reviews_count = Column(Integer, default=0)

class Order(Base):
    __tablename__ = 'orders'
    
    id = Column(Integer, primary_key=True)
    order_id = Column(String(50), unique=True, nullable=False)  # Внешний ID
    user_id = Column(BigInteger, nullable=False)
    product_id = Column(Integer, nullable=False)
    quantity = Column(Integer, default=1)
    total_amount = Column(Float, nullable=False)
    status = Column(String(50), default='pending')  # pending, paid, delivered, cancelled, refunded
    payment_method = Column(String(50))
    payment_id = Column(String(100))  # ID платежа в платежной системе
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)
    delivered_at = Column(DateTime)
    
    # Для цифровых товаров
    delivery_data = Column(JSON, default={})  # Данные для доставки (ссылка, пароль и т.д.)
    is_auto = Column(Boolean, default=True)   # Автодоставка
    
    # Реферальная система
    referral_bonus_paid = Column(Boolean, default=False)
    referral_user_id = Column(BigInteger)  # Кому начислен бонус

class Transaction(Base):
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    transaction_id = Column(String(50), unique=True, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    amount = Column(Float, nullable=False)
    type = Column(String(50))  # deposit, withdraw, purchase, refund, referral, bonus
    status = Column(String(50), default='pending')  # pending, completed, failed
    description = Column(Text)
    metadata = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class Referral(Base):
    __tablename__ = 'referrals'
    
    id = Column(Integer, primary_key=True)
    referrer_id = Column(BigInteger, nullable=False)
    referred_id = Column(BigInteger, nullable=False, unique=True)
    level = Column(Integer, default=1)  # Уровень в реферальной системе
    earned = Column(Float, default=0.0)
    status = Column(String(50), default='active')
    registered_at = Column(DateTime, default=datetime.datetime.utcnow)

class SupportTicket(Base):
    __tablename__ = 'support_tickets'
    
    id = Column(Integer, primary_key=True)
    ticket_id = Column(String(20), unique=True, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    subject = Column(String(255))
    message = Column(Text, nullable=False)
    status = Column(String(50), default='open')  # open, answered, closed
    priority = Column(String(20), default='normal')  # low, normal, high, critical
    admin_id = Column(BigInteger)  # Кто взял тикет
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)
    messages = Column(JSON, default=[])  # История переписки

class Notification(Base):
    __tablename__ = 'notifications'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False)
    type = Column(String(50))  # order, payment, system, promo
    title = Column(String(255))
    message = Column(Text)
    is_read = Column(Boolean, default=False)
    data = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class PromoCode(Base):
    __tablename__ = 'promo_codes'
    
    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False)
    discount_type = Column(String(20))  # percent, fixed
    discount_value = Column(Float, nullable=False)
    min_order_amount = Column(Float, default=0.0)
    max_uses = Column(Integer, default=1)
    used_count = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    valid_from = Column(DateTime)
    valid_until = Column(DateTime)
    created_by = Column(BigInteger)  # admin id

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis для FSM и кэша
redis_client = redis.from_url(Config.REDIS_URL)
storage = RedisStorage(redis=redis_client)

# База данных
engine = create_engine(Config.DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(bind=engine)

# Aiogram
session = AiohttpSession()
bot = Bot(
    token=Config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML"),
    session=session
)
dp = Dispatcher(storage=storage)

# Роутеры
main_router = Router()
admin_router = Router()
payment_router = Router()
dp.include_routers(main_router, admin_router, payment_router)

# ==================== УТИЛИТЫ ====================
class Utils:
    """Утилиты для работы бота"""
    
    @staticmethod
    async def get_db() -> Session:
        """Получение сессии БД"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    @staticmethod
    def generate_referral_code(user_id: int) -> str:
        """Генерация реферального кода"""
        return hashlib.md5(f"ref_{user_id}_{datetime.datetime.now().timestamp()}".encode()).hexdigest()[:8].upper()
    
    @staticmethod
    def format_price(price: float) -> str:
        """Форматирование цены"""
        return f"{price:,.2f} ₽".replace(",", " ")
    
    @staticmethod
    async def send_notification(user_id: int, title: str, message: str, notification_type: str = "system"):
        """Отправка уведомления пользователю"""
        try:
            await bot.send_message(
                user_id,
                f"🔔 <b>{title}</b>\n\n{message}",
                disable_notification=False
            )
            
            # Сохранение в БД
            async with SessionLocal() as db:
                notification = Notification(
                    user_id=user_id,
                    type=notification_type,
                    title=title,
                    message=message
                )
                db.add(notification)
                await db.commit()
                
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
    
    @staticmethod
    async def create_order_invoice(product, user_id: int, quantity: int = 1) -> dict:
        """Создание счета на оплату"""
        order_id = f"ORDER_{int(datetime.datetime.now().timestamp())}_{user_id}"
        total = product.price * quantity
        
        return {
            "order_id": order_id,
            "user_id": user_id,
            "product_id": product.id,
            "quantity": quantity,
            "total_amount": total,
            "description": f"Покупка: {product.name} x{quantity}"
        }

# ==================== КЛАВИАТУРЫ ====================
class Keyboards:
    """Клавиатуры бота"""
    
    @staticmethod
    def main_menu() -> InlineKeyboardMarkup:
        """Главное меню"""
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🛒 Каталог", callback_data="catalog"),
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
        )
        builder.row(
            InlineKeyboardButton(text="💰 Пополнить баланс", callback_data="deposit"),
            InlineKeyboardButton(text="📦 Мои покупки", callback_data="my_orders"),
        )
        builder.row(
            InlineKeyboardButton(text="👥 Реферальная система", callback_data="referral"),
            InlineKeyboardButton(text="🆘 Поддержка", callback_data="support"),
        )
        builder.row(
            InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings"),
            InlineKeyboardButton(text="ℹ️ О боте", callback_data="about"),
        )
        return builder.as_markup()
    
    @staticmethod
    def catalog_menu(categories: list) -> InlineKeyboardMarkup:
        """Меню каталога"""
        builder = InlineKeyboardBuilder()
        
        for category in categories:
            builder.button(text=f"📁 {category['name']}", callback_data=f"category_{category['id']}")
        
        builder.row(
            InlineKeyboardButton(text="🔍 Поиск товара", callback_data="search"),
            InlineKeyboardButton(text="🎁 Акции", callback_data="promotions"),
        )
        builder.row(
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
        )
        
        return builder.as_markup()
    
    @staticmethod
    def product_menu(product_id: int, in_stock: bool = True) -> InlineKeyboardMarkup:
        """Меню товара"""
        builder = InlineKeyboardBuilder()
        
        if in_stock:
            builder.button(text="🛒 Купить сейчас", callback_data=f"buy_{product_id}")
            builder.button(text="💰 Купить с баланса", callback_data=f"buy_balance_{product_id}")
        
        builder.button(text="📋 Описание", callback_data=f"desc_{product_id}")
        builder.button(text="⭐ Отзывы", callback_data=f"reviews_{product_id}")
        
        builder.row(
            InlineKeyboardButton(text="🔙 Назад в каталог", callback_data="catalog"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
        )
        
        return builder.as_markup()
    
    @staticmethod
    def payment_methods() -> InlineKeyboardMarkup:
        """Методы оплаты"""
        builder = InlineKeyboardBuilder()
        
        builder.row(
            InlineKeyboardButton(text="💳 Банковская карта", callback_data="pay_card"),
            InlineKeyboardButton(text="🥝 ЮMoney", callback_data="pay_yoomoney"),
        )
        builder.row(
            InlineKeyboardButton(text="🔶 ЮKassa", callback_data="pay_yookassa"),
            InlineKeyboardButton(text="₿ Криптовалюта", callback_data="pay_crypto"),
        )
        builder.row(
            InlineKeyboardButton(text="📱 QIWI", callback_data="pay_qiwi"),
            InlineKeyboardButton(text="🌐 СБП", callback_data="pay_sbp"),
        )
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data="deposit_back"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
        )
        
        return builder.as_markup()
    
    @staticmethod
    def profile_menu(user_data: dict) -> InlineKeyboardMarkup:
        """Меню профиля"""
        builder = InlineKeyboardBuilder()
        
        builder.row(
            InlineKeyboardButton(text="💰 Баланс: {:.2f} ₽".format(user_data['balance']), callback_data="balance_info"),
        )
        builder.row(
            InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
            InlineKeyboardButton(text="🎁 Промокод", callback_data="promo_activate"),
        )
        builder.row(
            InlineKeyboardButton(text="📱 Контакты", callback_data="contacts"),
            InlineKeyboardButton(text="✏️ Изменить данные", callback_data="edit_profile"),
        )
        builder.row(
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
        )
        
        return builder.as_markup()
    
    @staticmethod
    def referral_menu(ref_code: str) -> InlineKeyboardMarkup:
        """Реферальное меню"""
        builder = InlineKeyboardBuilder()
        
        builder.row(
            InlineKeyboardButton(text="📋 Мои рефералы", callback_data="my_refs"),
            InlineKeyboardButton(text="📈 Статистика", callback_data="ref_stats"),
        )
        builder.row(
            InlineKeyboardButton(text="💰 Вывод средств", callback_data="withdraw"),
            InlineKeyboardButton(text="🎁 Бонусы", callback_data="ref_bonuses"),
        )
        
        # Кнопка для копирования реферальной ссылки
        builder.row(
            InlineKeyboardButton(
                text="🔗 Скопировать ссылку", 
                callback_data=f"copy_ref_{ref_code}"
            )
        )
        
        builder.row(
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
        )
        
        return builder.as_markup()
    
    @staticmethod
    def support_menu() -> InlineKeyboardMarkup:
        """Меню поддержки"""
        builder = InlineKeyboardBuilder()
        
        builder.row(
            InlineKeyboardButton(text="📨 Создать тикет", callback_data="create_ticket"),
            InlineKeyboardButton(text="📋 Мои тикеты", callback_data="my_tickets"),
        )
        builder.row(
            InlineKeyboardButton(text="📞 Связаться с менеджером", url="https://t.me/manager_username"),
            InlineKeyboardButton(text="📚 FAQ", callback_data="faq"),
        )
        builder.row(
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
        )
        
        return builder.as_markup()
    
    @staticmethod
    def admin_menu() -> InlineKeyboardMarkup:
        """Админ меню"""
        builder = InlineKeyboardBuilder()
        
        builder.row(
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
            InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"),
        )
        builder.row(
            InlineKeyboardButton(text="🛒 Товары", callback_data="admin_products"),
            InlineKeyboardButton(text="📦 Заказы", callback_data="admin_orders"),
        )
        builder.row(
            InlineKeyboardButton(text="💰 Финансы", callback_data="admin_finance"),
            InlineKeyboardButton(text="🎁 Промокоды", callback_data="admin_promos"),
        )
        builder.row(
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
            InlineKeyboardButton(text="🆘 Поддержка", callback_data="admin_support"),
        )
        builder.row(
            InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin_settings"),
        )
        
        return builder.as_markup()

# ==================== СОСТОЯНИЯ (FSM) ====================
class Form(StatesGroup):
    """Состояния для FSM"""
    waiting_for_support_message = State()
    waiting_for_promo_code = State()
    waiting_for_withdraw_amount = State()
    waiting_for_withdraw_method = State()
    waiting_for_product_search = State()
    
    # Админ состояния
    admin_waiting_broadcast = State()
    admin_waiting_product_name = State()
    admin_waiting_product_price = State()
    admin_waiting_product_description = State()

# ==================== ОСНОВНЫЕ ХЭНДЛЕРЫ ====================
@main_router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    await state.clear()
    
    user_id = message.from_user.id
    args = message.text.split()
    
    # Проверка реферальной ссылки
    referral_code = None
    if len(args) > 1:
        referral_code = args[1]
    
    async with SessionLocal() as db:
        # Проверка существования пользователя
        user = db.query(User).filter(User.user_id == user_id).first()
        
        if not user:
            # Регистрация нового пользователя
            referral_code_used = None
            if referral_code:
                referrer = db.query(User).filter(User.referral_code == referral_code).first()
                if referrer:
                    referral_code_used = referrer.user_id
            
            new_user = User(
                user_id=user_id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
                language_code=message.from_user.language_code,
                referral_code=Utils.generate_referral_code(user_id),
                referred_by=referral_code_used,
                settings={
                    "notifications": True,
                    "language": "ru",
                    "theme": "dark"
                }
            )
            
            db.add(new_user)
            await db.commit()
            
            # Начисление бонуса рефереру
            if referral_code_used:
                referrer.balance += 100  # Бонус за приглашение
                referrer.successful_refs += 1
                
                # Создание реферальной записи
                referral = Referral(
                    referrer_id=referral_code_used,
                    referred_id=user_id,
                    level=1
                )
                db.add(referral)
                
                # Уведомление рефереру
                await Utils.send_notification(
                    referral_code_used,
                    "🎉 Новый реферал!",
                    f"Пользователь @{message.from_user.username} зарегистрировался по вашей ссылке!\n"
                    f"На ваш баланс начислено 100 ₽"
                )
            
            await message.answer(
                f"🎉 <b>Добро пожаловать, {message.from_user.first_name}!</b>\n\n"
                f"🤖 <b>Digital Shop Bot</b> - лучший бот для покупки цифровых товаров!\n\n"
                f"💎 <b>Ваши преимущества:</b>\n"
                f"• Мгновенная доставка товаров\n"
                f"• Поддержка 24/7\n"
                f"• Реферальная система до 3 уровней\n"
                f"• Безопасные платежи\n\n"
                f"🎁 <b>Бонус за регистрацию:</b> 50 ₽ на баланс!",
                reply_markup=Keyboards.main_menu()
            )
            
            # Начисление бонуса
            new_user.balance += 50
            await db.commit()
            
        else:
            # Пользователь уже существует
            user.last_activity = datetime.datetime.utcnow()
            await db.commit()
            
            await message.answer(
                f"👋 <b>С возвращением, {user.first_name}!</b>\n\n"
                f"Ваш баланс: {user.balance:.2f} ₽\n"
                f"Всего покупок: {user.orders_count}\n\n"
                f"Выберите действие:",
                reply_markup=Keyboards.main_menu()
            )

@main_router.callback_query(F.data == "main_menu")
async def callback_main_menu(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    
    async with SessionLocal() as db:
        user = db.query(User).filter(User.user_id == callback.from_user.id).first()
        
        await callback.message.edit_text(
            f"🏠 <b>Главное меню</b>\n\n"
            f"👤 Пользователь: {user.first_name}\n"
            f"💰 Баланс: {user.balance:.2f} ₽\n"
            f"🎯 Рефералов: {user.successful_refs}\n\n"
            f"Выберите раздел:",
            reply_markup=Keyboards.main_menu()
        )

# ==================== КАТАЛОГ ====================
@main_router.callback_query(F.data == "catalog")
async def callback_catalog(callback: CallbackQuery):
    """Каталог товаров"""
    async with SessionLocal() as db:
        # Получение категорий
        categories = db.query(Product.category).distinct().all()
        categories_list = []
        
        for i, cat in enumerate(categories, 1):
            if cat[0]:  # Проверка на None
                count = db.query(Product).filter(
                    Product.category == cat[0],
                    Product.is_active == True
                ).count()
                
                categories_list.append({
                    "id": i,
                    "name": cat[0],
                    "count": count
                })
        
        if not categories_list:
            await callback.message.edit_text(
                "📦 <b>Каталог товаров</b>\n\n"
                "На данный момент товары отсутствуют.\n"
                "Пожалуйста, проверьте позже.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
                ])
            )
            return
        
        text = "📦 <b>Каталог товаров</b>\n\n"
        text += "Выберите категорию:\n\n"
        
        for cat in categories_list:
            text += f"📁 {cat['name']} - {cat['count']} товаров\n"
        
        await callback.message.edit_text(
            text,
            reply_markup=Keyboards.catalog_menu(categories_list)
        )

@main_router.callback_query(F.data.startswith("category_"))
async def callback_category(callback: CallbackQuery):
    """Товары в категории"""
    category_id = int(callback.data.split("_")[1])
    
    async with SessionLocal() as db:
        # Получение категории
        categories = db.query(Product.category).distinct().all()
        category_name = categories[category_id-1][0] if category_id <= len(categories) else None
        
        if not category_name:
            await callback.answer("Категория не найдена!")
            return
        
        # Получение товаров в категории
        products = db.query(Product).filter(
            Product.category == category_name,
            Product.is_active == True
        ).order_by(Product.created_at.desc()).limit(20).all()
        
        if not products:
            await callback.message.edit_text(
                f"📁 <b>Категория: {category_name}</b>\n\n"
                "В этой категории пока нет товаров.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="catalog")]
                ])
            )
            return
        
        # Создание карусели товаров
        builder = InlineKeyboardBuilder()
        
        for product in products:
            builder.row(
                InlineKeyboardButton(
                    text=f"{product.name} - {product.price:.2f} ₽",
                    callback_data=f"product_{product.id}"
                )
            )
        
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data="catalog"),
            InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
        )
        
        await callback.message.edit_text(
            f"📁 <b>Категория: {category_name}</b>\n\n"
            f"Найдено товаров: {len(products)}\n\n"
            "Выберите товар:",
            reply_markup=builder.as_markup()
        )

@main_router.callback_query(F.data.startswith("product_"))
async def callback_product(callback: CallbackQuery):
    """Информация о товаре"""
    product_id = int(callback.data.split("_")[1])
    
    async with SessionLocal() as db:
        product = db.query(Product).filter(Product.id == product_id).first()
        
        if not product:
            await callback.answer("Товар не найден!")
            return
        
        # Формирование описания
        description = f"<b>{product.name}</b>\n\n"
        description += f"{product.description}\n\n" if product.description else ""
        description += f"💵 <b>Цена:</b> {product.price:.2f} ₽\n"
        
        if product.stock >= 0:
            description += f"📦 <b>В наличии:</b> {product.stock} шт.\n"
        else:
            description += "📦 <b>В наличии:</b> ∞\n"
        
        description += f"⭐ <b>Рейтинг:</b> {product.rating}/5 ({product.reviews_count} отзывов)\n"
        description += f"🛒 <b>Продано:</b> {product.sales_count} шт.\n\n"
        
        if product.attributes:
            description += "<b>Характеристики:</b>\n"
            for key, value in product.attributes.items():
                description += f"• {key}: {value}\n"
        
        # Кнопки
        in_stock = product.stock != 0
        
        if product.image_url:
            try:
                await callback.message.delete()
                await callback.message.answer_photo(
                    photo=product.image_url,
                    caption=description,
                    reply_markup=Keyboards.product_menu(product_id, in_stock)
                )
                return
            except:
                pass
        
        await callback.message.edit_text(
            description,
            reply_markup=Keyboards.product_menu(product_id, in_stock)
        )

@main_router.callback_query(F.data.startswith("buy_"))
async def callback_buy_product(callback: CallbackQuery):
    """Покупка товара"""
    data = callback.data.split("_")
    product_id = int(data[1])
    use_balance = len(data) > 2 and data[2] == "balance"
    
    async with SessionLocal() as db:
        user = db.query(User).filter(User.user_id == callback.from_user.id).first()
        product = db.query(Product).filter(Product.id == product_id).first()
        
        if not product or not product.is_active:
            await callback.answer("Товар недоступен!")
            return
        
        if product.stock == 0:
            await callback.answer("Товар закончился!")
            return
        
        if use_balance:
            # Покупка с баланса
            if user.balance < product.price:
                await callback.answer("Недостаточно средств на балансе!")
                return
            
            # Создание заказа
            order = await Utils.create_order_invoice(product, user.user_id)
            order_obj = Order(
                order_id=order["order_id"],
                user_id=user.user_id,
                product_id=product.id,
                total_amount=product.price,
                status="paid",
                payment_method="balance",
                is_auto=True
            )
            
            # Списание средств
            user.balance -= product.price
            user.total_spent += product.price
            user.orders_count += 1
            
            # Обновление статистики товара
            product.sales_count += 1
            product.total_revenue += product.price
            if product.stock > 0:
                product.stock -= 1
            
            db.add(order_obj)
            await db.commit()
            
            # Доставка товара
            await deliver_product(callback.from_user.id, order_obj, product)
            
            await callback.answer("✅ Товар успешно куплен! Проверьте свои покупки.")
            await callback_main_menu(callback, None)
            
        else:
            # Выбор способа оплаты
            builder = InlineKeyboardBuilder()
            
            builder.row(
                InlineKeyboardButton(text="💳 Картой", callback_data=f"pay_card_{product_id}"),
                InlineKeyboardButton(text="🥝 ЮMoney", callback_data=f"pay_yoomoney_{product_id}"),
            )
            builder.row(
                InlineKeyboardButton(text="🔶 ЮKassa", callback_data=f"pay_yookassa_{product_id}"),
                InlineKeyboardButton(text="📱 QIWI", callback_data=f"pay_qiwi_{product_id}"),
            )
            builder.row(
                InlineKeyboardButton(text="🔙 Назад", callback_data=f"product_{product_id}"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")
            )
            
            await callback.message.edit_text(
                f"🛒 <b>Покупка: {product.name}</b>\n\n"
                f"💵 Сумма к оплате: {product.price:.2f} ₽\n\n"
                f"Выберите способ оплаты:",
                reply_markup=builder.as_markup()
            )

# ==================== ПРОФИЛЬ ====================
@main_router.callback_query(F.data == "profile")
async def callback_profile(callback: CallbackQuery):
    """Личный кабинет"""
    async with SessionLocal() as db:
        user = db.query(User).filter(User.user_id == callback.from_user.id).first()
        
        if not user:
            await callback.answer("Пользователь не найден!")
            return
        
        # Статистика
        total_orders = db.query(Order).filter(
            Order.user_id == user.user_id,
            Order.status == "paid"
        ).count()
        
        total_spent = user.total_spent
        
        text = f"👤 <b>Личный кабинет</b>\n\n"
        text += f"🆔 ID: {user.user_id}\n"
        text += f"👤 Имя: {user.first_name}\n"
        if user.username:
            text += f"📱 Username: @{user.username}\n"
        text += f"📅 Регистрация: {user.registration_date.strftime('%d.%m.%Y')}\n"
        text += f"💰 Баланс: {user.balance:.2f} ₽\n"
        text += f"🛒 Всего покупок: {total_orders}\n"
        text += f"💳 Всего потрачено: {total_spent:.2f} ₽\n"
        text += f"👥 Приглашено: {user.successful_refs} чел.\n"
        text += f"🎁 Заработано: {user.total_earned:.2f} ₽\n\n"
        
        if user.referral_code:
            text += f"🔗 Реферальный код: <code>{user.referral_code}</code>\n"
            text += f"🔗 Реферальная ссылка: https://t.me/{callback.message.bot.username}?start={user.referral_code}"
        
        await callback.message.edit_text(
            text,
            reply_markup=Keyboards.profile_menu({
                "balance": user.balance
            })
        )

# ==================== РЕФЕРАЛЬНАЯ СИСТЕМА ====================
@main_router.callback_query(F.data == "referral")
async def callback_referral(callback: CallbackQuery):
    """Реферальная система"""
    async with SessionLocal() as db:
        user = db.query(User).filter(User.user_id == callback.from_user.id).first()
        
        if not user:
            await callback.answer("Пользователь не найден!")
            return
        
        # Статистика рефералов
        referrals = db.query(Referral).filter(
            Referral.referrer_id == user.user_id
        ).all()
        
        # Расчет заработка по уровням
        level_stats = {1: 0, 2: 0, 3: 0}
        for ref in referrals:
            if ref.level in level_stats:
                level_stats[ref.level] += 1
        
        text = f"👥 <b>Реферальная система</b>\n\n"
        text += f"🔗 Ваш реферальный код: <code>{user.referral_code}</code>\n"
        text += f"🔗 Реферальная ссылка: https://t.me/{callback.message.bot.username}?start={user.referral_code}\n\n"
        
        text += f"📊 <b>Статистика:</b>\n"
        text += f"• Всего рефералов: {len(referrals)}\n"
        text += f"• Уровень 1: {level_stats[1]} чел.\n"
        text += f"• Уровень 2: {level_stats[2]} чел.\n"
        text += f"• Уровень 3: {level_stats[3]} чел.\n"
        text += f"• Заработано: {user.total_earned:.2f} ₽\n\n"
        
        text += f"💰 <b>Условия:</b>\n"
        text += f"• За каждого реферала 1 уровня: {Config.REFERRAL_PERCENT}% от его покупок\n"
        text += f"• За каждого реферала 2 уровня: {Config.REFERRAL_PERCENT//2}% от его покупок\n"
        text += f"• За каждого реферала 3 уровня: {Config.REFERRAL_PERCENT//4}% от его покупок\n\n"
        
        text += f"🎁 <b>Бонусы:</b>\n"
        text += f"• За приглашение друга: 100 ₽ каждому\n"
        text += f"• Минимальный вывод: {Config.MIN_WITHDRAW} ₽"
        
        await callback.message.edit_text(
            text,
            reply_markup=Keyboards.referral_menu(user.referral_code)
        )

@main_router.callback_query(F.data.startswith("copy_ref_"))
async def callback_copy_ref(callback: CallbackQuery):
    """Копирование реферальной ссылки"""
    ref_code = callback.data.split("_")[2]
    ref_link = f"https://t.me/{callback.message.bot.username}?start={ref_code}"
    
    await callback.answer(
        f"Ссылка скопирована!\n\n{ref_link}",
        show_alert=True
    )

# ==================== ПОДДЕРЖКА ====================
@main_router.callback_query(F.data == "support")
async def callback_support(callback: CallbackQuery):
    """Техническая поддержка"""
    async with SessionLocal() as db:
        # Получение открытых тикетов пользователя
        tickets = db.query(SupportTicket).filter(
            SupportTicket.user_id == callback.from_user.id,
            SupportTicket.status == "open"
        ).count()
        
        text = f"🆘 <b>Техническая поддержка</b>\n\n"
        text += f"Здесь вы можете получить помощь по работе с ботом.\n\n"
        
        if tickets > 0:
            text += f"📨 У вас есть открытые тикеты: {tickets}\n"
        
        text += f"\n<b>Доступные опции:</b>\n"
        text += f"• Создать новый тикет\n"
        text += f"• Просмотреть мои тикеты\n"
        text += f"• Связаться с менеджером\n"
        text += f"• Читать FAQ\n\n"
        
        text += f"⏱ <b>Время ответа:</b> до 15 минут\n"
        text += f"🕒 <b>Рабочие часы:</b> 24/7"
        
        await callback.message.edit_text(
            text,
            reply_markup=Keyboards.support_menu()
        )

@main_router.callback_query(F.data == "create_ticket")
async def callback_create_ticket(callback: CallbackQuery, state: FSMContext):
    """Создание тикета в поддержку"""
    await state.set_state(Form.waiting_for_support_message)
    
    await callback.message.edit_text(
        "📨 <b>Создание тикета в поддержку</b>\n\n"
        "Пожалуйста, опишите вашу проблему подробно.\n"
        "Укажите:\n"
        "• Что случилось?\n"
        "• Когда это произошло?\n"
        "• Номер заказа (если есть)\n\n"
        "⚠️ <i>Отправляйте только текст, без фото/файлов.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="support")]
        ])
    )

@main_router.message(Form.waiting_for_support_message)
async def process_support_message(message: Message, state: FSMContext):
    """Обработка сообщения в поддержку"""
    if len(message.text) < 10:
        await message.answer("Сообщение слишком короткое. Опишите проблему подробнее.")
        return
    
    async with SessionLocal() as db:
        # Создание тикета
        ticket_id = f"TICKET_{int(datetime.datetime.now().timestamp())}_{message.from_user.id}"
        
        ticket = SupportTicket(
            ticket_id=ticket_id,
            user_id=message.from_user.id,
            subject="Проблема с ботом",
            message=message.text,
            status="open",
            priority="normal",
            messages=[{
                "from": "user",
                "text": message.text,
                "time": datetime.datetime.utcnow().isoformat()
            }]
        )
        
        db.add(ticket)
        await db.commit()
        
        # Уведомление админов
        for admin_id in Config.ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🆘 <b>Новый тикет #{ticket_id}</b>\n\n"
                    f"👤 Пользователь: @{message.from_user.username or 'нет'}\n"
                    f"🆔 ID: {message.from_user.id}\n\n"
                    f"📝 <b>Сообщение:</b>\n{message.text}\n\n"
                    f"📅 Время: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📨 Ответить", callback_data=f"admin_reply_{ticket.id}")]
                    ])
                )
            except:
                pass
        
        await state.clear()
        
        await message.answer(
            f"✅ <b>Тикет создан!</b>\n\n"
            f"🆔 Номер тикета: <code>{ticket_id}</code>\n"
            f"📅 Время: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Мы ответим вам в течение 15 минут.\n"
            f"Вы можете просмотреть статус тикета в разделе 'Мои тикеты'.",
            reply_markup=Keyboards.support_menu()
        )

# ==================== АДМИН ПАНЕЛЬ ====================
@admin_router.message(Command("admin"))
async def cmd_admin(message: Message):
    """Админ панель"""
    if message.from_user.id not in Config.ADMIN_IDS:
        return
    
    async with SessionLocal() as db:
        # Статистика
        total_users = db.query(User).count()
        total_products = db.query(Product).count()
        total_orders = db.query(Order).count()
        
        # Финансы
        total_revenue = db.query(Order.total_amount).filter(
            Order.status == "paid"
        ).all()
        total_revenue_sum = sum([r[0] for r in total_revenue]) if total_revenue else 0
        
        today = datetime.datetime.utcnow().date()
        today_orders = db.query(Order).filter(
            Order.status == "paid",
            Order.created_at >= today
        ).count()
        
        text = f"⚙️ <b>Админ панель</b>\n\n"
        text += f"📊 <b>Статистика:</b>\n"
        text += f"• Пользователей: {total_users}\n"
        text += f"• Товаров: {total_products}\n"
        text += f"• Заказов: {total_orders}\n"
        text += f"• Заказов сегодня: {today_orders}\n"
        text += f"• Общая выручка: {total_revenue_sum:.2f} ₽\n\n"
        
        # Последние заказы
        recent_orders = db.query(Order).order_by(
            Order.created_at.desc()
        ).limit(5).all()
        
        if recent_orders:
            text += f"🔄 <b>Последние заказы:</b>\n"
            for order in recent_orders:
                product = db.query(Product).filter(Product.id == order.product_id).first()
                text += f"• {product.name if product else 'Товар'} - {order.total_amount} ₽\n"
        
        await message.answer(
            text,
            reply_markup=Keyboards.admin_menu()
        )

@admin_router.callback_query(F.data == "admin_stats")
async def callback_admin_stats(callback: CallbackQuery):
    """Статистика для админа"""
    if callback.from_user.id not in Config.ADMIN_IDS:
        return
    
    async with SessionLocal() as db:
        # Детальная статистика
        today = datetime.datetime.utcnow().date()
        week_ago = today - datetime.timedelta(days=7)
        
        # Пользователи
        total_users = db.query(User).count()
        new_users_today = db.query(User).filter(
            User.registration_date >= today
        ).count()
        active_users = db.query(User).filter(
            User.last_activity >= week_ago
        ).count()
        
        # Заказы
        total_orders = db.query(Order).count()
        today_orders = db.query(Order).filter(
            Order.created_at >= today
        ).count()
        week_orders = db.query(Order).filter(
            Order.created_at >= week_ago
        ).count()
        
        # Финансы
        total_revenue = db.scalar(
            db.query(db.func.sum(Order.total_amount)).filter(
                Order.status == "paid"
            )
        ) or 0
        
        today_revenue = db.scalar(
            db.query(db.func.sum(Order.total_amount)).filter(
                Order.status == "paid",
                Order.created_at >= today
            )
        ) or 0
        
        week_revenue = db.scalar(
            db.query(db.func.sum(Order.total_amount)).filter(
                Order.status == "paid",
                Order.created_at >= week_ago
            )
        ) or 0
        
        text = f"📊 <b>Детальная статистика</b>\n\n"
        
        text += f"👥 <b>Пользователи:</b>\n"
        text += f"• Всего: {total_users}\n"
        text += f"• Новых сегодня: {new_users_today}\n"
        text += f"• Активных за неделю: {active_users}\n\n"
        
        text += f"🛒 <b>Заказы:</b>\n"
        text += f"• Всего: {total_orders}\n"
        text += f"• Сегодня: {today_orders}\n"
        text += f"• За неделю: {week_orders}\n\n"
        
        text += f"💰 <b>Финансы:</b>\n"
        text += f"• Общая выручка: {total_revenue:.2f} ₽\n"
        text += f"• Выручка сегодня: {today_revenue:.2f} ₽\n"
        text += f"• Выручка за неделю: {week_revenue:.2f} ₽\n\n"
        
        # Топ товаров
        top_products = db.query(
            Product.name, Product.sales_count, Product.total_revenue
        ).order_by(
            Product.sales_count.desc()
        ).limit(5).all()
        
        if top_products:
            text += f"🏆 <b>Топ товаров:</b>\n"
            for i, (name, sales, revenue) in enumerate(top_products, 1):
                text += f"{i}. {name[:20]}... - {sales} шт. ({revenue:.0f} ₽)\n"
        
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📈 Графики", callback_data="admin_charts")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
            ])
        )

# ==================== ДОСТАВКА ТОВАРОВ ====================
async def deliver_product(user_id: int, order: Order, product: Product):
    """Автоматическая доставка товара"""
    
    try:
        # Для цифровых товаров
        if product.file_url:
            # Если есть файл - отправляем
            delivery_message = (
                f"✅ <b>Ваш заказ #{order.order_id} доставлен!</b>\n\n"
                f"🎁 <b>Товар:</b> {product.name}\n"
                f"💵 <b>Сумма:</b> {order.total_amount:.2f} ₽\n"
                f"📅 <b>Время:</b> {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
            )
            
            if product.file_password:
                delivery_message += f"🔐 <b>Пароль от архива:</b> <code>{product.file_password}</code>\n\n"
            
            delivery_message += "⬇️ <b>Ссылка для скачивания:</b>\n"
            
            # Отправка сообщения с файлом
            try:
                await bot.send_message(
                    user_id,
                    delivery_message,
                    disable_web_page_preview=False
                )
                
                # Отправка файла или ссылки
                if product.file_url.startswith(('http', 'https')):
                    await bot.send_message(
                        user_id,
                        f"🔗 <a href='{product.file_url}'>Скачать товар</a>\n\n"
                        f"<i>Если ссылка не работает, обратитесь в поддержку.</i>"
                    )
                else:
                    # Локальный файл
                    await bot.send_document(
                        user_id,
                        FSInputFile(product.file_url),
                        caption="📎 Ваш файл"
                    )
                    
            except Exception as e:
                logger.error(f"Failed to send file: {e}")
                
                # Если не удалось отправить файл, сохраняем данные в заказ
                async with SessionLocal() as db:
                    order.delivery_data = {
                        "file_url": product.file_url,
                        "password": product.file_password,
                        "delivery_attempts": 1
                    }
                    await db.commit()
                
                await bot.send_message(
                    user_id,
                    f"📦 <b>Товар готов к выдаче!</b>\n\n"
                    f"Обратитесь в поддержку для получения файла.\n"
                    f"🆔 Номер заказа: <code>{order.order_id}</code>"
                )
        
        else:
            # Для товаров без файла (ключи, аккаунты и т.д.)
            delivery_data = {
                "type": "text",
                "delivered_at": datetime.datetime.now().isoformat()
            }
            
            # Генерация данных товара (например, ключа)
            if product.attributes.get("type") == "license_key":
                key = generate_license_key()
                delivery_data["key"] = key
                
                delivery_message = (
                    f"✅ <b>Ваш заказ #{order.order_id} доставлен!</b>\n\n"
                    f"🎁 <b>Товар:</b> {product.name}\n"
                    f"🔑 <b>Ключ:</b> <code>{key}</code>\n\n"
                    f"💵 <b>Сумма:</b> {order.total_amount:.2f} ₽\n"
                    f"📅 <b>Время:</b> {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"<i>Сохраните ключ в надежном месте!</i>"
                )
                
            elif product.attributes.get("type") == "account":
                login = generate_account_login()
                password = generate_password()
                delivery_data["login"] = login
                delivery_data["password"] = password
                
                delivery_message = (
                    f"✅ <b>Ваш заказ #{order.order_id} доставлен!</b>\n\n"
                    f"🎁 <b>Товар:</b> {product.name}\n"
                    f"👤 <b>Логин:</b> <code>{login}</code>\n"
                    f"🔐 <b>Пароль:</b> <code>{password}</code>\n\n"
                    f"💵 <b>Сумма:</b> {order.total_amount:.2f} ₽\n"
                    f"📅 <b>Время:</b> {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"<i>Рекомендуем сменить пароль после входа!</i>"
                )
                
            else:
                delivery_message = (
                    f"✅ <b>Ваш заказ #{order.order_id} доставлен!</b>\n\n"
                    f"🎁 <b>Товар:</b> {product.name}\n"
                    f"💵 <b>Сумма:</b> {order.total_amount:.2f} ₽\n"
                    f"📅 <b>Время:</b> {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"<i>Для получения товара обратитесь в поддержку.</i>"
                )
            
            # Сохранение данных доставки
            async with SessionLocal() as db:
                order.delivery_data = delivery_data
                order.delivered_at = datetime.datetime.utcnow()
                await db.commit()
            
            await bot.send_message(user_id, delivery_message)
        
        # Начисление реферального бонуса
        await process_referral_bonus(order)
        
        # Отправка уведомления о доставке
        await Utils.send_notification(
            user_id,
            "🎉 Заказ доставлен!",
            f"Ваш заказ #{order.order_id} успешно доставлен.\n"
            f"Товар: {product.name}"
        )
        
    except Exception as e:
        logger.error(f"Delivery error: {e}")
        await bot.send_message(
            user_id,
            f"⚠️ <b>Ошибка при доставке заказа #{order.order_id}</b>\n\n"
            f"Пожалуйста, обратитесь в поддержку для решения проблемы."
        )

def generate_license_key() -> str:
    """Генерация лицензионного ключа"""
    import random
    import string
    
    parts = []
    for _ in range(4):
        part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
        parts.append(part)
    
    return "-".join(parts)

def generate_account_login() -> str:
    """Генерация логина для аккаунта"""
    import random
    import string
    
    prefix = random.choice(["user", "player", "gamer", "account"])
    numbers = ''.join(random.choices(string.digits, k=6))
    
    return f"{prefix}{numbers}"

def generate_password() -> str:
    """Генерация пароля"""
    import random
    import string
    
    length = random.randint(8, 12)
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    
    return ''.join(random.choices(chars, k=length))

# ==================== РЕФЕРАЛЬНЫЕ ВЫПЛАТЫ ====================
async def process_referral_bonus(order: Order):
    """Начисление реферального бонуса"""
    async with SessionLocal() as db:
        # Находим пользователя
        user = db.query(User).filter(User.user_id == order.user_id).first()
        if not user or not user.referred_by:
            return
        
        # Получаем реферера
        referrer = db.query(User).filter(User.user_id == user.referred_by).first()
        if not referrer:
            return
        
        # Расчет бонуса
        bonus_percent = Config.REFERRAL_PERCENT
        bonus_amount = (order.total_amount * bonus_percent) / 100
        
        # Начисление бонуса рефереру
        referrer.balance += bonus_amount
        referrer.total_earned += bonus_amount
        
        # Обновление реферальной записи
        referral = db.query(Referral).filter(
            Referral.referred_id == user.user_id
        ).first()
        
        if referral:
            referral.earned += bonus_amount
        
        # Создание транзакции
        transaction = Transaction(
            transaction_id=f"REF_{int(datetime.datetime.now().timestamp())}",
            user_id=referrer.user_id,
            amount=bonus_amount,
            type="referral",
            status="completed",
            description=f"Реферальный бонус от заказа #{order.order_id}",
            metadata={
                "order_id": order.order_id,
                "referred_user_id": user.user_id,
                "percent": bonus_percent,
                "purchase_amount": order.total_amount
            }
        )
        
        db.add(transaction)
        
        # Обновление заказа
        order.referral_bonus_paid = True
        order.referral_user_id = referrer.user_id
        
        await db.commit()
        
        # Уведомление рефереру
        await Utils.send_notification(
            referrer.user_id,
            "💰 Получен реферальный бонус!",
            f"За покупку вашего реферала вам начислен бонус {bonus_amount:.2f} ₽\n"
            f"Заказ: #{order.order_id}\n"
            f"Ваш баланс: {referrer.balance:.2f} ₽"
        )

# ==================== WEBHOOK ====================
async def on_startup(dispatcher: Dispatcher):
    """Действия при запуске"""
    logger.info("Bot starting...")
    
    # Установка webhook
    webhook_url = Config.WEBHOOK_URL + Config.WEBHOOK_PATH
    await bot.set_webhook(
        url=webhook_url,
        drop_pending_updates=True,
        secret_token="YOUR_SECRET_TOKEN"
    )
    
    logger.info(f"Webhook set to {webhook_url}")

async def on_shutdown(dispatcher: Dispatcher):
    """Действия при выключении"""
    logger.info("Bot shutting down...")
    await bot.session.close()
    await dispatcher.storage.close()

# ==================== ЗАПУСК БОТА ====================
async def main():
    """Основная функция запуска"""
    
    # Запуск через webhook (для VPS)
    app = web.Application()
    
    # Настройка webhook
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token="YOUR_SECRET_TOKEN"
    )
    
    webhook_requests_handler.register(app, path=Config.WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    
    # Запуск
    await on_startup(dp)
    
    try:
        # Для локального тестирования можно использовать polling
        # await dp.start_polling(bot)
        
        # Для продакшена - webhook
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        
        logger.info("Bot started successfully!")
        
        # Бесконечный цикл
        await asyncio.Event().wait()
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
    finally:
        await on_shutdown(dp)

if __name__ == "__main__":

    asyncio.run(main())
