package com.abish.lighterlive

import android.app.PendingIntent
import android.appwidget.AppWidgetManager
import android.appwidget.AppWidgetProvider
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.graphics.Color
import android.widget.RemoteViews
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.concurrent.Executors

class LighterWidgetProvider : AppWidgetProvider() {
    companion object {
        const val ACTION_REFRESH = "com.abish.lighterlive.ACTION_REFRESH"
        private const val TELEMETRY_URL =
            "https://crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com/live-telemetry.json?t="
        private val executor = Executors.newSingleThreadExecutor()
    }

    override fun onUpdate(context: Context, appWidgetManager: AppWidgetManager, appWidgetIds: IntArray) {
        updateAllWidgets(context, appWidgetManager, appWidgetIds)
    }

    override fun onReceive(context: Context, intent: Intent) {
        super.onReceive(context, intent)
        if (intent.action == ACTION_REFRESH) {
            val mgr = AppWidgetManager.getInstance(context)
            val ids = mgr.getAppWidgetIds(ComponentName(context, LighterWidgetProvider::class.java))
            updateAllWidgets(context, mgr, ids)
        }
    }

    private fun updateAllWidgets(context: Context, mgr: AppWidgetManager, ids: IntArray) {
        if (ids.isEmpty()) return
        executor.execute {
            try {
                val conn = (URL(TELEMETRY_URL + System.currentTimeMillis()).openConnection() as HttpURLConnection).apply {
                    connectTimeout = 6000
                    readTimeout = 6000
                }
                val raw = conn.inputStream.bufferedReader().readText()
                conn.disconnect()

                val root = JSONObject(raw)
                val acct = root.optJSONObject("lighterAccount") ?: return@execute
                val eq = acct.optString("total_asset_value", "0").toDoubleOrNull() ?: 0.0
                val col = acct.optString("collateral", "0").toDoubleOrNull() ?: 0.0
                val openPnl = eq - col

                val posArr = acct.optJSONArray("positions")
                val posParts = mutableListOf<String>()
                if (posArr != null) {
                    for (i in 0 until posArr.length()) {
                        val p = posArr.getJSONObject(i)
                        val sz = p.optString("position", "0").toDoubleOrNull() ?: 0.0
                        val ords = p.optInt("open_order_count", 0)
                        if (kotlin.math.abs(sz) > 0.0 || ords > 0) {
                            val sym = p.optString("symbol", "MKT")
                            val sign = if (p.optInt("sign", 1) < 0) "-" else "+"
                            val upnl = p.optString("unrealized_pnl", "0").toDoubleOrNull() ?: 0.0
                            val dir = if (sz == 0.0) "Q($ords)" else "$sign${String.format(Locale.US, "%.4f", sz)}"
                            posParts.add("$sym $dir (${String.format(Locale.US, "%+.3f", upnl)})")
                        }
                    }
                }
                val posText = if (posParts.isEmpty()) "Flat • Waiting" else posParts.joinToString("  |  ")
                val ts = SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(Date())

                for (id in ids) {
                    val rv = RemoteViews(context.packageName, R.layout.widget_lighter)
                    rv.setTextViewText(R.id.wEq, String.format(Locale.US, "$%.4f", eq))
                    rv.setTextViewText(
                        R.id.wSub,
                        String.format(Locale.US, "Col $%.4f  •  Open %+.4f", col, openPnl)
                    )
                    rv.setTextViewText(R.id.wPos, posText)
                    rv.setTextViewText(R.id.wTime, ts)
                    rv.setTextColor(R.id.wEq, if (openPnl >= 0) Color.parseColor("#10b981") else Color.parseColor("#f8fafc"))

                    val openApp = PendingIntent.getActivity(
                        context, 0, Intent(context, MainActivity::class.java),
                        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
                    )
                    rv.setOnClickPendingIntent(R.id.wRoot, openApp)

                    val refreshIntent = Intent(context, LighterWidgetProvider::class.java).apply { action = ACTION_REFRESH }
                    val refreshPi = PendingIntent.getBroadcast(
                        context, 1, refreshIntent,
                        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
                    )
                    rv.setOnClickPendingIntent(R.id.wRefresh, refreshPi)

                    mgr.updateAppWidget(id, rv)
                }
            } catch (_: Exception) {
            }
        }
    }
}
