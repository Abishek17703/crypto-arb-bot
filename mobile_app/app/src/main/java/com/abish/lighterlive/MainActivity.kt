package com.abish.lighterlive

import android.annotation.SuppressLint
import android.graphics.Color
import android.os.Bundle
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.appcompat.app.AppCompatActivity

class MainActivity : AppCompatActivity() {
    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.statusBarColor = Color.parseColor("#000000")
        window.navigationBarColor = Color.parseColor("#000000")

        val webView = WebView(this)
        webView.setBackgroundColor(Color.parseColor("#000000"))
        webView.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            cacheMode = WebSettings.LOAD_NO_CACHE
            allowFileAccess = true
        }
        webView.webViewClient = WebViewClient()
        webView.loadUrl("file:///android_asset/lighter_live_dashboard.html")
        setContentView(webView)
    }
}
